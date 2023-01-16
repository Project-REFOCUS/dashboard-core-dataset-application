from common.constants import entity_key, cache_id, constants
from common.service import cached_request, cached_query
from common.utils import ensure_int, start_of_day
from datetime import datetime, timedelta
from entity.abstract import ResourceEntity
from .api import TwitterApi

import csv
import io
import os
import re

URL = 'https://docs.google.com/spreadsheets/d/1v5uSOskUD1En0iY3WcR4zCP_nogYT-8IPjMr9mI_414/pub?single=true&output=csv'


def get_search_start_time():
    today = datetime.today()
    start_time = today - timedelta(days=6)
    start_time_input = os.getenv(constants.twitter_api_search_start_time)
    if start_time_input is not None and re.compile(constants.date_regex).match(start_time_input):
        try:
            start_time = datetime.strptime(start_time_input, constants.datetime_format)
        except ValueError:
            pass

    return start_time


def get_twitter_id(record, field):
    return ensure_int(record[field])


def get_reply_count(record, field):
    return record[field]['reply_count']


def get_retweet_count(record, field):
    return record[field]['retweet_count']


def get_like_count(record, field):
    return record[field]['like_count']


def get_tweet_timestamp(record, field):
    return datetime.strptime(record[field], '%Y-%m-%dT%H:%M:%S.000Z')


class Tweets(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.calendar_date, entity_key.twitter_account]

    def get_tweet_url(self, record, field):
        twitter_account_entity = self.dependencies_map[entity_key.twitter_account]
        author_id = record['author_id']
        twitter_id = record[field]
        twitter_account = twitter_account_entity.get_cached_value(author_id)
        username = twitter_account['username'] if twitter_account is not None else twitter_account
        return f'https://twitter.com/{username}/status/{twitter_id}' if username is not None else ''

    def get_twitter_account_id(self, record, field):
        twitter_account_entity = self.dependencies_map[entity_key.twitter_account]
        author_id = record[field]
        twitter_account = twitter_account_entity.get_cached_value(author_id)
        return twitter_account['id'] if twitter_account is not None else None

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        datetime_timestamp = get_tweet_timestamp(record, field)
        iso_date = str(datetime_timestamp.date())
        return calendar_date_entity.get_cached_value(iso_date)['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'tweets'
        self.fields = [
            {'field': 'id', 'column': 'twitter_id', 'data': get_twitter_id},
            {'field': 'text', 'column': 'tweet'},
            {'field': 'public_metrics', 'column': 'replies', 'data': get_reply_count},
            {'field': 'public_metrics', 'column': 'retweets', 'data': get_retweet_count},
            {'field': 'public_metrics', 'column': 'likes', 'data': get_like_count},
            # {'field': 'text','column': 'hashtag'},
            {'field': 'id', 'column': 'link', 'data': self.get_tweet_url},
            {'field': 'created_at', 'data': get_tweet_timestamp},
            {'field': 'author_id', 'column': 'twitter_account_id', 'data': self.get_twitter_account_id},
            {'field': 'created_at', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id}
        ]
        self.cacheable_fields = ['twitter_id']
        self.search_start_time = get_search_start_time()
        self.search_end_time = None

    def load_cache(self):
        joined_table = f'{self.table_name},calendar_date'
        start_date = str((datetime.today() - timedelta(days=7)).date())
        where_clause = f'{self.table_name}.calendar_date_id = calendar_date.id and calendar_date.date > {start_date}'
        records = cached_query(entity_key.twitter_tweets, joined_table, self.cacheable_fields, where_clause)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in self.cacheable_fields:
                self.record_cache[str(record[field])] = record

    def get_cache(self):
        self.load_cache()
        return self.record_cache

    def skip_record(self, record):
        return self.record_cache is not None and record['id'] in self.record_cache

    def fetch(self):
        self.records = []

    def has_data(self):
        if len(self.records) == 0 and self.search_start_time < start_of_day(datetime.today()):
            self.get_tweets()

        return len(self.records) != 0

    def get_tweets(self):
        request = cached_request(cache_id.twitter_accounts, 'GET', URL)
        accounts_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')))

        self.records = []
        self.updates = []

        api = TwitterApi()
        api.authenticate()

        if not api.is_authenticated():
            print('Could not authenticate with the Twitter Api')
            return

        today = datetime.today()
        ninety_days_after_start = self.search_start_time + timedelta(days=90)
        self.search_end_time = ninety_days_after_start if today > ninety_days_after_start else today

        usernames = []
        for data in accounts_data:
            username = data['Twitter handle']
            usernames.append(f'from:{username}')
            username_query = ' OR '.join(usernames)
            if len(usernames) >= 10:
                tweets = api.get_tweets_by_username(username_query, self.search_start_time, self.search_end_time)
                self.records.extend(tweets)
                usernames = []

    def after_save(self):
        super().after_save()

        self.search_start_time = self.search_end_time
        self.records = []

