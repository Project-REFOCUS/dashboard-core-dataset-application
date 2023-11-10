from common.constants import entity_key
from entity.abstract import ResourceEntity

import json
import requests

API_URL = 'https://data.census.gov/api/access/data/table' + \
    '?id=ACSDT5Y2020.B01003&g=040XX00US{}$1500000'

class BlockGroupPopulation2020(ResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_block_group
        ]

    def format_block_group(self, block_group):
        return block_group.replace('ñ','n').lower()

    def get_block_group_id(self, record, field):
        block_group_entity = self.dependencies_map[entity_key.census_block_group]
        block_group = block_group_entity.get_cached_value(self.format_block_group(record[field]))
        return block_group['id'] if block_group else None

    def __init__(self):
        super().__init__()

        self.table_name = 'block_group_population_2020'
        self.fields = [
            {'field': 'population'},
            {'field': 'block_group', 'column': 'block_group_id', 'data': self.get_block_group_id},
        ]
        self.cacheable_fields = ['block_group_id']

    def skip_record(self, record):
        return self.record_cache and self.format_block_group(record['block_group']) in self.record_cache

    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            block_group_entity = self.dependencies_map[entity_key.census_block_group]
            
            for record in records:
                for field in self.cacheable_fields:
                    block_group_record = block_group_entity.get_cached_value(record[field])
                    formatted_block_group = block_group_entity.format_block_group(block_group_record['name'])
                    self.record_cache[formatted_block_group] = record

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
            # Note that the first element is improper
            data.pop(0)
            for record in data:
                if record[5] not in duplicate_set:
                    self.records.append({'population': record[2], 'block_group': record[5]})