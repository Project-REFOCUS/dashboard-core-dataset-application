from common.constants import entity_key, cache_id
from common.service import cached_request
from common.utils import ensure_int
from datetime import datetime, timedelta
from entity.abstract import ResourceEntity
from .api import TwitterApi

import csv
import io

URL = 'https://docs.google.com/spreadsheets/d/1v5uSOskUD1En0iY3WcR4zCP_nogYT-8IPjMr9mI_414/pub?single=true&output=csv'


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
        twitter_account_cache = self.dependencies_cache[entity_key.twitter_account]
        author_id = record['author_id']
        twitter_id = record[field]
        username = twitter_account_cache[author_id]['username'] if author_id in twitter_account_cache else None
        return f'https://twitter.com/{username}/status/{twitter_id}' if username is not None else ''

    def get_twitter_account_id(self, record, field):
        twitter_account_cache = self.dependencies_cache[entity_key.twitter_account]
        author_id = record[field]
        return twitter_account_cache[author_id]['id'] if author_id in twitter_account_cache else None

    def get_calendar_date_id(self, record, field):
        calendar_date_cache = self.dependencies_cache[entity_key.calendar_date]
        datetime_timestamp = get_tweet_timestamp(record, field)
        iso_date = str(datetime_timestamp.date())
        return calendar_date_cache[iso_date]['id']

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

    def load_cache(self):
        joined_table = f'{self.table_name},calendar_date'
        start_date = str((datetime.today() - timedelta(days=7)).date())
        where_clause = f'{self.table_name}.calendar_date_id = calendar_date.id and calendar_date.date > {start_date}'
        records = self.mysql_client.select(joined_table, fields=self.cacheable_fields, where=where_clause)
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
        datetime_format = '%Y-%m-%dT00:00:00.000Z'
        start_time = datetime.strftime(today - timedelta(days=5), datetime_format)
        end_time = datetime.strftime(today, datetime_format)

        for data in accounts_data:
            username = data['Twitter handle']

            tweets = api.get_tweets_by_username(username, start_time, end_time)
            self.records.extend(tweets)
