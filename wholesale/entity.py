from common.constants import entity_key, cache_id
from common.service import cached_request
from entity.abstract import ResourceEntity
from datetime import date, timedelta

import re
import json
import requests

API_URL = 'https://data.cityofnewyork.us/resource/87fx-28ei.json' + \
    '?$select=`market`,`bic_number`,`account_name`,`application_type`,`disposition_date`,`postcode`,`effective_date`,`expiration_date`' + \
    '&$where=disposition_date = \'{}\' &$limit=10000&$offset={}'

YYYY_MM_DD_PATTERN = re.compile('\\d{4}-\\d{1,2}-\\d{1,2}')

class WholesaleMarket(ResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_us_zipcode,
            entity_key.calendar_date,
            entity_key.wholesale_market_app_type
        ]

    def get_zipcode_id(self, record, field):
        zipcode_entity = self.dependencies_map[entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(record[field])
        return zipcode['id'] if zipcode else None
    
    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        iso_date = None
        
        if YYYY_MM_DD_PATTERN.match(record[field]) is not None:
            iso_date = str(datetime.strptime(record[field], '%Y-%m-%d').date())

        calendar_date = calendar_date_entity.get_cached_value(iso_date)
        return calendar_date['id'] if calendar_date is not None else calendar_date
    
    def get_market_app_type_id(self, record, field):
        app_type_entity = self.dependencies_map[entity_key.wholesale_market_app_type]
        app_type = app_entity.get_cached_value(record[field])
        return app['id'] if app else None
    
    def get_public_id(self, record, field):
        return record['field'] + record['disposition_date']

    def __init__(self):
        super().__init__()

        self.table_name = 'wholesale_market'
        self.fields = [
            {'field': 'market'},
            {'field': 'bic_number', 'column': 'public_id', 'data': self.get_public_id},
            {'field': 'account_name'},
            {'field': 'application_type', 'column': 'market_application_type_id', 'data': self.get_market_app_type_id},
            {'field': 'disposition_date', 'column': 'disposition_date_id', 'data': self.get_calendar_date_id},
            {'field': 'postcode', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
            {'field': 'effective_date', 'column': 'effective_date_id','data': self.get_calendar_date_id},
            {'field': 'expiration_date', 'column': 'expiration_date_id','data': self.get_calendar_date_id},
        ]
        self.cacheable_fields = ['public_id']
    
    def skip_record(self, record):

        if 'disposition_date' in record is None:
            return true

        if self.record_cache and self.get_public_id(record,'bic_number') in self.record_cache:
            return true
        
        if 'application_type' in record:
            if record['application_type'] and YYYY_MM_DD_PATTERN.match(record['application_type']) is not None:
                return true
        else:
            return true
            
        return false

    def fetch(self):
        self.records = []

        tomorrows_date = date(date.today().year, date.today().month, date.today().day + 1)
        current_date = date(2020, 1, 1)

        while current_date < tomorrows_date:
            continue_fetching = True
            offset = 0
            while continue_fetching:
                request_url = API_URL.format(
                    current_date,
                    offset
                )
                #request = cached_request(cache_id)
                records = json.loads(requests.request('GET', request_url).content.decode('utf-8'))
                self.records.extend(records)
                continue_fetching = len(records) == 1000
                offset += (1000 if continue_fetching else 0)

            current_date += timedelta(days=1)