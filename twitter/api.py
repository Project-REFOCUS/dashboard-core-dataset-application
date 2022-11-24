from requests.auth import HTTPBasicAuth

import requests
import json
import time
import os

BASE_URL = 'https://api.twitter.com'


class TwitterApi:
    requests_remaining = 300
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
        TwitterApi.requests_remaining = int(response.headers['x-rate-limit-remaining'])
        return response

    def get_users_by_usernames(self, usernames):
        usernames_param = ','.join(usernames)
        response = self.send_request(f'/2/users/by?usernames={usernames_param}')
        return json.loads(response.content.decode('utf-8'))['data'] if response.status_code == 200 else []

    def get_tweets_by_username(self, username, start_time, end_time, tweets=None, next_token=None):
        query_param = f'query=from:{username}'
        max_results_param = 'max_results=100'
        tweet_fields_param = 'tweet.fields=author_id,created_at,public_metrics'
        start_time_param = f'start_time={start_time}'
        end_time_param = f'end_time={end_time}'
        query = f'{query_param}&{max_results_param}&{start_time_param}&{end_time_param}&{tweet_fields_param}'

        if next_token is not None:
            query = f'{query}&next_token={next_token}'

        time.sleep(2)
        url = f'/2/tweets/search/recent?{query}'
        response = self.send_request(url)

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
            self.get_tweets_by_username(username, start_time, end_time, tweets, twitter_response['meta']['next_token'])

        return tweets
