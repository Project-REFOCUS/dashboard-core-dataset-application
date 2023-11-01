from common.constants import entity_key
from entity.abstract import ResourceEntity
from datetime import datetime, timedelta

import json
import requests

API_URL = 'https://data.cityofnewyork.us/resource/dsg6-ifza.json' + \
    '?$select=`centername`,`legalname`,`zipcode`,`status`,`childcaretype`' + \
    '&$where=inspectiondate >= \'{}\' and inspectiondate < \'{}\'&$limit=10000&$offset={}' + \
    '&$order=inspectiondate'

class ChildCareCenter(ResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_us_zipcode,
            entity_key.childcare_center_type
        ]

    def get_zipcode_id(self, record, field):
        zipcode_entity = self.dependencies_map[entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(record[field])
        return zipcode['id'] if zipcode else None

    def get_caretype_id(self, record, field):
        childcare_type_entity = self.dependencies_map[entity_key.childcare_center_type]
        return childcare_type_entity.get_cached_value(record[field])['id']
    
    def to_lowercase(self, record, field):
        return record[field].lower()
    
    def __init__(self):
        super().__init__()

        self.table_name = 'childcare_center'
        self.fields = [
            {'field': 'centername', 'column': 'center_name', 'data': self.to_lowercase},
            {'field': 'legalname', 'column': 'legal_name'},
            {'field': 'zipcode', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
            {'field': 'status'},
            {'field': 'childcaretype', 'column': 'childcare_type_id', 'data': self.get_caretype_id},
        ]
        self.cacheable_fields = ['center_name']

    def skip_record(self, record):
        return self.record_cache and record['centername'].lower() in self.record_cache

    def fetch(self):
        self.records = []
        center_name_set = set()
        
        today_date = datetime.today()
        begin_date = datetime(2020, 1, 1)

        continue_fetching = True
        offset = 0
        while continue_fetching:
            request_url = API_URL.format(
                begin_date.isoformat(),
                today_date.isoformat(),
                offset
            )
            records = json.loads(requests.request('GET', request_url).content.decode('utf-8'))
            
            for record in records:
                if 'zipcode' in record:
                    center_name = record['centername'].lower()
                    if center_name not in center_name_set:
                        self.records.append(record)
                        center_name_set.add(center_name)
            
            continue_fetching = len(records) == 10000
            offset += (10000 if continue_fetching else 0)