from common.constants import entity_key, cache_id
from entity.abstract import ResourceEntity
from datetime import date, datetime

import re
import json
import requests

API_URL = 'https://data.cityofnewyork.us/resource/87fx-28ei.json' + \
    '?$select=`market`,`bic_number`,`account_name`,`application_type`,`disposition_date`,`postcode`,`expiration_date`' + \
    '&$where=disposition_date >= \'{}\' and disposition_date <= \'{}\' &$limit=10000&$offset={}' + '&$order=disposition_date'

YYYY_MM_DD_PATTERN = re.compile('\\d{4}-\\d{1,2}-\\d{1,2}')
ZIPCODE_PATTERN = re.compile('^\\d{5}$')
ALPHA_ONLY_PATTERN = re.compile('^[A-Za-z\\s]+$')

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
        app_type = app_type_entity.get_cached_value(record[field])
        return app_type['id'] if app_type else None
    
    def create_public_id(self, record, field):
        return record[field] + record['disposition_date']

    def __init__(self):
        super().__init__()

        self.table_name = 'wholesale_market'
        self.fields = [
            {'field': 'market'},
            {'field': 'bic_number', 'column': 'public_id', 'data': self.create_public_id},
            {'field': 'account_name'},
            {'field': 'application_type', 'column': 'market_application_type_id', 'data': self.get_market_app_type_id},
            {'field': 'disposition_date', 'column': 'disposition_date_id', 'data': self.get_calendar_date_id},
            {'field': 'postcode', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
            {'field': 'expiration_date', 'column': 'expiration_date_id','data': self.get_calendar_date_id},
        ]
        self.cacheable_fields = ['public_id']
    
    def skip_record(self, record):
        response = False
        
        if 'disposition_date' not in record or record['disposition_date'] is None or YYYY_MM_DD_PATTERN.match(record['disposition_date']) is None:
            response = True
        elif self.record_cache and self.create_public_id(record,'bic_number') in self.record_cache:
            response = True
        elif 'expiration_date' not in record or record['expiration_date'] is None or YYYY_MM_DD_PATTERN.match(record['expiration_date']) is None:
            response = True
        elif datetime.strptime(record['expiration_date'], '%Y-%m-%d').date() > date.today():
            response = True
        elif datetime.strptime(record['expiration_date'], '%Y-%m-%d').date() < datetime.strptime(record['disposition_date'], '%Y-%m-%d').date():
            repsonse = True
        elif 'application_type' not in record or record['application_type'] is None or ALPHA_ONLY_PATTERN.match(record['application_type']) is None:
            response = True
        elif 'postcode' not in record or record['postcode'] is None or ZIPCODE_PATTERN.match(record['postcode']) is None:
            response = True
        
        return response


    def fetch(self):
        self.records = []
        public_id_set = set()

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
                if 'disposition_date' in record:
                    public_id = self.create_public_id(record,'bic_number')
                    if public_id not in public_id_set:
                        self.records.append(record)
                        public_id_set.add(public_id)

            continue_fetching = len(records) == 10000
            offset += (10000 if continue_fetching else 0)