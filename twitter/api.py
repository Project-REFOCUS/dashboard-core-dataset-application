from common.constants import constants
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth

import requests
import json
import time
import os

BASE_URL = 'https://api.twitter.com'


class TwitterApi:
    search_tweets_requests_remaining = 300
    count_tweets_requests_remaining = 300
    tweets_remaining = 10000000

    def __init__(self):
        self.api_client = os.getenv('TWITTER_API_CLIENT')
        self.api_secret = os.getenv('TWITTER_API_SECRET')
        self.api_token = None

    def authenticate(self):
        url = '/oauth2/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {'grant_type': 'client_credentials'}
        auth = HTTPBasicAuth(self.api_client, self.api_secret)
        response = requests.request('POST', f'{BASE_URL}{url}', headers=headers, data=body, auth=auth)

        if response.status_code == 200:
            self.api_token = json.loads(response.content.decode('utf-8'))['access_token']

    def is_authenticated(self):
        return self.api_token is not None

    def send_request(self, url):
        headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {self.api_token}'}
        response = requests.request('GET', f'{BASE_URL}{url}', headers=headers)
        return response

    def get_users_by_usernames(self, usernames):
        usernames_param = ','.join(usernames)
        response = self.send_request(f'/2/users/by?usernames={usernames_param}')
        return json.loads(response.content.decode('utf-8'))['data'] if response.status_code == 200 else []

    def get_tweet_counts_by_usernames(self, usernames, start_time, end_time, next_token=None):
        start_time_param = f'start_time={datetime.strftime(start_time, constants.datetime_format)}'
        end_time_param = f'end_time={datetime.strftime(end_time, constants.datetime_format)}'
        query = f'query={usernames}&{start_time_param}&{end_time_param}'
        if next_token is not None:
            query = f'{query}&next_token={next_token}'

        url = f'{constants.twitter_api_tweet_count}'

        response = self.send_request(f'{url}?{query}')
        TwitterApi.count_tweets_requests_remaining = int(response.headers['x-rate-limit-remaining'])
        twitter_response = json.loads(response.content.decode('utf-8'))
        tweet_count = twitter_response['meta']['total_tweet_count']
        if 'next_token' in twitter_response['meta']:
            return tweet_count + self.get_tweet_counts_by_usernames(usernames, start_time, end_time, next_token=next_token)

        return tweet_count

    def get_tweets_by_username(self, username_query, start_time, end_time, tweets=None, next_token=None):
        today = datetime.today()
        full_search = today > start_time + timedelta(days=7)
        query_param = f'query={username_query}'
        max_results_param = 'max_results=500' if full_search else 'max_results=100'
        tweet_fields_param = 'tweet.fields=author_id,created_at,public_metrics'
        start_time_param = f'start_time={datetime.strftime(start_time, constants.datetime_format)}'
        end_time_param = f'end_time={datetime.strftime(end_time, constants.datetime_format)}'
        query = f'{query_param}&{max_results_param}&{start_time_param}&{end_time_param}&{tweet_fields_param}'

        if next_token is not None:
            query = f'{query}&next_token={next_token}'

        time.sleep(2)
        url = constants.twitter_api_full_search if full_search else constants.twitter_api_recent_search

        response = self.send_request(f'{url}?{query}')
        TwitterApi.search_tweets_requests_remaining = int(response.headers['x-rate-limit-remaining'])

        if tweets is None:
            tweets = []

        twitter_response = json.loads(response.content.decode('utf-8'))

        if response.status_code != 200:
            print(f'Non success response for url: {url}')
            print(response.reason)
            return tweets
        elif 'data' in twitter_response:
            tweets.extend(twitter_response['data'])

        if 'next_token' in twitter_response['meta']:
            self.get_tweets_by_username(username_query, start_time, end_time, tweets, twitter_response['meta']['next_token'])

        return tweets
