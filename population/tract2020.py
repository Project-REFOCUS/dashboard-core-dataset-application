from common.constants import entity_key
from entity.abstract import ResourceEntity

import json
import requests

API_URL = 'https://data.census.gov/api/access/data/table' + \
    '?id=ACSDT5Y2020.B01003&g=040XX00US{}$1400000'

class TractPopulation2020(ResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_tract
        ]

    def format_census_tract(self, subject_tract):
        return subject_tract.replace('ñ','n').lower()

    def get_tract_id(self, record, field):
        tract_entity = self.dependencies_map[entity_key.census_tract]
        tract = tract_entity.get_cached_value(self.format_census_tract(record[field]))
        return tract['id'] if tract else None
    
    def __init__(self):
        super().__init__()

        self.table_name = 'census_tract_population_2020'
        self.fields = [
            {'field': 'population'},
            {'field': 'census_tract', 'column': 'census_tract_id', 'data': self.get_tract_id},
        ]
        self.cacheable_fields = ['census_tract_id']

    def skip_record(self, record):
        return self.record_cache and self.format_census_tract(record['census_tract']) in self.record_cache

    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            tract_entity = self.dependencies_map[entity_key.census_tract]
            
            for record in records:
                for field in self.cacheable_fields:
                    tract_record = tract_entity.get_cached_value(record[field])
                    formatted_tract = tract_entity.format_census_tract(tract_record['name'])
                    self.record_cache[formatted_tract] = record

    def fetch(self):
        self.records = []
        PAGEABLE_MAX = 56
        duplicate_set = set()
        
        for i in range(PAGEABLE_MAX):
            i += 1
            
            if(i in ({3,7,14,43,52})):
                continue
            
            offset = i if i >= 10 else '0' + str(i)
            
            request_url = API_URL.format(
                offset
            )
            
            response = json.loads(requests.request('GET', request_url).content.decode('utf-8'))
            data = response['response']['data']
            # Note: the first element is improper
            data.pop(0)
            for record in data:
                if record[5] not in duplicate_set:
                    self.records.append({'population': record[2], 'census_tract': record[5]})