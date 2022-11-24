from common.constants import entity_key, cache_id
from common.service import cached_request
from entity.abstract import ResourceEntity
from .api import TwitterApi

import csv
import io

URL = 'https://docs.google.com/spreadsheets/d/1v5uSOskUD1En0iY3WcR4zCP_nogYT-8IPjMr9mI_414/pub?single=true&output=csv'


def capitalize(record, field, record_cache):
    return record[field].capitalize()


def lowercase(record, field, record_cache):
    return record[field].lower()


def insert_twitter_ids(accounts_by_username, users):
    for user in users:
        accounts_by_username[user['username'].lower()]['twitter_id'] = user['id']


class TwitterAccountType(ResourceEntity):

    @staticmethod
    def dependencies():
        return []

    def __init__(self):
        super().__init__()

        self.table_name = 'twitter_account_type'
        self.fields = [{'field': 'name', 'column': 'name', 'data': capitalize}]

    def load_cache(self):
        cacheable_fields = ['name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cacheable_fields:
                self.record_cache[record[field].lower()] = record

    def skip_record(self, record):
        return self.record_cache is not None and record['name'] in self.record_cache

    def fetch(self):
        request = cached_request(cache_id.twitter_accounts, 'GET', URL)
        raw_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')))

        self.records = []
        self.updates = []
        account_type_set = set()

        for data in raw_data:
            account_type_set.add(data['Organization type'])

        for name in list(account_type_set):
            self.records.append({'name': name})


class TwitterAccount(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.twitter_account_type]

    def get_account_type_id(self, record, field):
        account_type_cache = self.dependencies_cache[entity_key.twitter_account_type]
        return account_type_cache[record[field]]['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'twitter_account'
        self.fields = [
            {'field': 'name'},
            {'field': 'twitter_id'},
            {'field': 'username', 'data': lowercase},
            {'field': 'type', 'column': 'twitter_account_type_id', 'data': self.get_account_type_id}
        ]

    def load_cache(self):
        cacheable_fields = ['id', 'twitter_id']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cacheable_fields:
                self.record_cache[str(record[field])] = record

    def skip_record(self, record):
        return self.record_cache is not None and record['twitter_id'] in self.record_cache

    def fetch(self):
        request = cached_request(cache_id.twitter_accounts, 'GET', URL)
        raw_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')))

        self.records = []
        self.updates = []

        list_of_usernames = []

        api = TwitterApi()
        api.authenticate()

        if not api.is_authenticated():
            print('Could not authenticate with the Twitter Api')
            return

        accounts_by_username = {}

        for data in raw_data:
            username = data['Twitter handle'].lower()
            accounts_by_username[username] = {
                'type': data['Organization type'],
                'name': data['Organization name'],
                'username': username
            }
            list_of_usernames.append(username)
            if len(list_of_usernames) >= 100:
                users = api.get_users_by_usernames(list_of_usernames)
                insert_twitter_ids(accounts_by_username, users)
                list_of_usernames = []

        if len(list_of_usernames) > 0:
            users = api.get_users_by_usernames(list_of_usernames)
            insert_twitter_ids(accounts_by_username, users)

        for username in accounts_by_username.keys():
            if 'twitter_id' in accounts_by_username[username]:
                self.records.append(accounts_by_username[username])
