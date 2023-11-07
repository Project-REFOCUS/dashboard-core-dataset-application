from common.constants import entity_key, cache_id
from entity.abstract import ResourceEntity
from datetime import date, datetime

import re
import json
import requests

API_URL = 'https://data.cityofnewyork.us/resource/87fx-28ei.json' + \
    '?$select=`market`,`bic_number`,`account_name`,`application_type`,`postcode`' + \
    '&$where=disposition_date >= \'{}\' and disposition_date <= \'{}\' &$limit=10000&$offset={}' + '&$order=disposition_date'

ZIPCODE_PATTERN = re.compile('^\\d{5}$')
ALPHA_ONLY_PATTERN = re.compile('^[A-Za-z\\s]+$')

class WholesaleMarket(ResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_us_zipcode,
            entity_key.wholesale_market_app_type
        ]

    def get_zipcode_id(self, record, field):
        zipcode_entity = self.dependencies_map[entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(record[field])
        return zipcode['id'] if zipcode else None
    
    def get_market_app_type_id(self, record, field):
        app_type_entity = self.dependencies_map[entity_key.wholesale_market_app_type]
        app_type = app_type_entity.get_cached_value(record[field])
        return app_type['id'] if app_type else None

    def __init__(self):
        super().__init__()

        self.table_name = 'wholesale_market'
        self.fields = [
            {'field': 'market'},
            {'field': 'account_name'},
            {'field': 'application_type', 'column': 'market_application_type_id', 'data': self.get_market_app_type_id},
            {'field': 'postcode', 'column': 'zipcode_id', 'data': self.get_zipcode_id}
        ]
        self.cacheable_fields = ['account_name']
    
    def skip_record(self, record):
        response = False

        if self.record_cache and record['account_name'].lower() in self.record_cache:
            response = True
        elif 'application_type' not in record or record['application_type'] is None or ALPHA_ONLY_PATTERN.match(record['application_type']) is None:
            response = True
        elif 'postcode' not in record or record['postcode'] is None or ZIPCODE_PATTERN.match(record['postcode']) is None:
            response = True
        
        return response
    
    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            for record in records:
                for field in self.cacheable_fields:
                    self.record_cache[str(record[field]).lower()] = record


    def fetch(self):
        self.records = []
        account_name_set = set()

        today_date = date.today()
        begin_date = date(2020, 1, 1)

        continue_fetching = True
        offset = 0
        while continue_fetching:
            request_url = API_URL.format(
                begin_date,
                today_date,
                offset
            )
            
            records = json.loads(requests.request('GET', request_url).content.decode('utf-8'))
            
            for record in records:
                if 'account_name' in record:
                    account_name = record['account_name'].lower()
                    if account_name not in account_name_set:
                        self.records.append(record)
                        account_name_set.add(account_name)

            continue_fetching = len(records) == 10000
            offset += (10000 if continue_fetching else 0)