from common.constants import entity_key
from common.service import cached_query
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

    def load_cache(self):
        cacheable_fields = ['word']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cacheable_fields:
                self.record_cache[record[field]] = record

    def get_cache(self):
        self.load_cache()
        return self.record_cache

    def skip_record(self, record):
        return self.record_cache is not None and record['term'] in self.record_cache

    def fetch(self):
        twitter_tweets_table = 'tweets'
        tweet_results = cached_query(entity_key.twitter_tweets, twitter_tweets_table, ['id', 'tweet'])

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
