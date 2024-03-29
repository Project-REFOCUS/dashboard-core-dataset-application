from common.constants import cache_id, entity_key
from common.service import cached_query
from datetime import datetime, timedelta
from entity.abstract import ResourceEntity

import re

ALPHABETIC = re.compile('^[a-z]+$')
NUMBER_PATTERN = re.compile('^\\d+$')
MENTION_PATTERN = re.compile('^@\\w+$')
EMPTY_STRING_PATTERN = re.compile('^\\s+$')
URL_PATTERN = re.compile('^https://\\w+\\.\\w+/?.*')
EMAIL_PATTERN = re.compile('^[\\w.]+@\\w+\\.[a-z]+$')
NON_ALPHANUMERIC_START = re.compile('^[^\\w+]+')
NON_ALPHANUMERIC_END = re.compile('[^\\w+]+$')
TERM_SPLIT_PATTERN = re.compile('\\s+|;|\\n+|…')


def match_pattern(pattern, term):
    return pattern.match(term) is not None


def is_number(term):
    return match_pattern(NUMBER_PATTERN, term)


def is_mention(term):
    return match_pattern(MENTION_PATTERN, term)


def is_empty_string(term):
    return term == '' or match_pattern(EMPTY_STRING_PATTERN, term)


def is_url(term):
    return match_pattern(URL_PATTERN, term)


def is_email(term):
    return match_pattern(EMAIL_PATTERN, term)


def is_meaningless(term):
    return len(term) <= 2


def should_be_ignored(term):
    return is_number(term) or is_mention(term) or is_empty_string(term) or\
        is_url(term) or is_email(term) or is_meaningless(term)


def non_alphabetic(term):
    return ALPHABETIC.match(term) is None


def sanitize(term):
    front_sanitized_term = re.sub(NON_ALPHANUMERIC_START, '', term.strip())
    partially_sanitized_term = re.sub(NON_ALPHANUMERIC_END, '', front_sanitized_term.strip())
    sanitized_term = partially_sanitized_term.replace('\'', '')

    return sanitized_term.lower()


# TODO: Further granulate to return portions of terms that are valid
def ignore_some(term):
    sub_terms = re.split('\\W', term)
    ignore = False
    for sub_term in sub_terms:
        sanitized_sub_term = sanitize(sub_term)
        ignore = ignore or is_number(sanitized_sub_term) or is_empty_string(sanitized_sub_term)
        if ignore:
            break

    return ignore


class TwitterTerms(ResourceEntity):

    @staticmethod
    def dependencies():
        return []

    def __init__(self):
        super().__init__()

        self.table_name = 'twitter_terms'
        self.fields = [{'field': 'term', 'column': 'word'}]
        self.cacheable_fields = ['word']

    def get_cache(self):
        self.load_cache()
        return self.record_cache

    def skip_record(self, record):
        return self.record_cache is not None and record['term'] in self.record_cache

    def fetch(self):
        tweets_table = 'tweets'
        joined_table = f'{tweets_table},calendar_date'
        start_date = str((datetime.today() - timedelta(days=7)).date())
        fields = [f'{tweets_table}.id as id', 'tweet']
        where_clause = f'{tweets_table}.calendar_date_id = calendar_date.id and calendar_date.date > "{start_date}"'
        tweet_results = cached_query(cache_id.tweets_by_text_and_id, joined_table, fields=fields, where=where_clause)

        self.records = []
        self.updates = []

        unique_terms = set()

        for tweet_object in tweet_results:
            terms = re.split('\\s+|;|\\n+|…', tweet_object['tweet'])

            for term in terms:
                sanitized_term = sanitize(term)
                # if not should_be_ignored(sanitized_term) and not ignore_some(sanitized_term):
                if not is_empty_string(sanitized_term) and not non_alphabetic(sanitized_term):
                    unique_terms.add(sanitized_term)

        for unique_term in list(unique_terms):
            self.records.append({'term': unique_term})


class TwitterTermsFrequency(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.twitter_tweets_terms]

    def __init__(self):
        super().__init__()

        self.table_name = 'twitter_terms_frequency'
        self.fields = [
            {'field': 'frequency'},
            {'field': 'twitter_terms_id'},
            {'field': 'tweets_id'}
        ]
        self.cacheable_fields = ['twitter_terms_id']

    def load_cache(self):
        joined_table = f'{self.table_name},tweets,calendar_date'
        start_date = str((datetime.today() - timedelta(days=7)).date())
        where_clause = f'{self.table_name}.tweets_id = tweets.id and tweets.calendar_date_id = calendar_date.id and calendar_date.date > "{start_date}"'
        fields = self.cacheable_fields + ['tweets_id']
        records = self.mysql_client.select(joined_table, fields=fields, where=where_clause)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in self.cacheable_fields:
                if record[field] not in self.record_cache:
                    self.record_cache[record[field]] = set()

                self.record_cache[record[field]].add(record['tweets_id'])

    def get_cache(self):
        if self.record_cache is None:
            self.load_cache()

        return self.record_cache

    def skip_record(self, record):
        return self.record_cache is not None and record['twitter_terms_id'] in self.record_cache and\
            record['tweets_id'] in self.record_cache[record['twitter_terms_id']]

    def fetch(self):
        tweets_table = 'tweets'
        joined_table = f'{tweets_table},calendar_date'
        start_date = str((datetime.today() - timedelta(days=7)).date())
        where_clause = f'{tweets_table}.calendar_date_id = calendar_date.id and calendar_date.date > "{start_date}"'
        tweet_results = cached_query(cache_id.tweets_by_text_and_id, joined_table, ['id', 'tweet'], where_clause)

        self.records = []
        self.updates = []

        term_frequency_map = {}

        for tweet_object in tweet_results:
            terms = re.split(TERM_SPLIT_PATTERN, tweet_object['tweet'])

            for term in terms:
                sanitized_term = sanitize(term)
                if not is_empty_string(sanitized_term) and not non_alphabetic(sanitized_term):
                    if tweet_object['id'] not in term_frequency_map:
                        term_frequency_map[tweet_object['id']] = {}

                    if sanitized_term not in term_frequency_map[tweet_object['id']]:
                        term_frequency_map[tweet_object['id']] = {
                            'term': sanitized_term,
                            'frequency': 0
                        }

                    term_frequency_map[tweet_object['id']]['frequency'] += 1

        twitter_terms_entity = self.dependencies_map[entity_key.twitter_tweets_terms]
        for tweet in tweet_results:
            tweet_id = tweet['id']

            if tweet_id in term_frequency_map:
                frequency_object = term_frequency_map[tweet_id]
                self.records.append({
                    'frequency': frequency_object['frequency'],
                    'twitter_terms_id': twitter_terms_entity.get_cached_value(frequency_object['term'])['id'],
                    'tweets_id': tweet_id
                })
