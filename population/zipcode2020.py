from common.constants import entity_key
from entity.abstract import ResourceEntity

import json
import requests

API_URL = 'https://data.census.gov/api/access/data/table' + \
    '?id=ACSDT5Y2020.B01003&g=040XX00US{}$8600000'

class ZipcodePopulation2020(ResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_us_zipcode,
        ]

    def get_zipcode_id(self, record, field):
        zipcode_value = record[field].split()[1]
        zipcode_entity = self.dependencies_map[entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(zipcode_value)
        return zipcode['id'] if zipcode else None
    
    def __init__(self):
        super().__init__()

        self.table_name = 'zipcode_population_2020'
        self.fields = [
            {'field': 'population'},
            {'field': 'zipcode', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
        ]
        self.cacheable_fields = ['zipcode_id']

    def skip_record(self, record):
        return self.record_cache and str(self.get_zipcode_id(record, 'zipcode')) in self.record_cache

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
                    duplicate_set.add(record[5])
                    self.records.append({'population': record[2], 'zipcode': record[5]})
                    