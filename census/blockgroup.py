from common.constants import entity_key
from common.utils import progress
from entity.abstract import ResourceEntity
from census.abstract import CensusPopulationResourceEntity

import requests
import json

FETCHED_RECORDS_THRESHOLD = 10000


def get_block_group_fips(record, field):
    [_, fips_code] = record[field].split('US')
    return fips_code


class BlockGroup(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_tract]
    
    @staticmethod
    def format_block_group(subject_block):
        return subject_block.replace(';',',').replace('├▒','ñ').replace('├│','ó').replace('├¡','í').replace('├í','á').replace('├╝','ü').lower()

    def get_census_tract_id(self, record, field):
        census_tract_entity = self.dependencies_map[entity_key.census_tract]
        census_tract_fips_code = record[field].split('US')[1][:-1]
        return census_tract_entity.get_cached_value(census_tract_fips_code)['id']

    def __init__(self):
        super().__init__()
        self.table_name = 'block_group'
        self.record_cache = {}
        self.fetch_complete = False

        self.fields = [
            {'field': 'name'},
            {'field': 'code', 'column': 'fips', 'data': get_block_group_fips},
            {'field': 'code', 'column': 'census_tract_id', 'data': self.get_census_tract_id}
        ]

        self.cacheable_fields = ['name', 'fips', 'id']

    def skip_record(self, record):
        return 'code' in record and '$' in record['code'] \
            or 'name' in record and self.format_block_group(record['name']) in self.record_cache
    
    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            for record in records:
                for field in self.cacheable_fields:
                    if field == 'name':
                        self.record_cache[self.format_block_group(str(record[field]))] = record
                    else:
                        self.record_cache[str(record[field])] = record

    def has_data(self):
        if self.fetch_complete is False:
            self.load_cache()
            self.fetch()
        return super().has_data()

    def fetch(self):
        base_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=7&showComponents=false'
        census_tract_cache = self.dependencies_map[entity_key.census_tract].record_cache
        resolved_census_tract_fips = set()

        self.records = []
        # record_count = len(census_tract_cache)
        records_fetched = 0
        for block_group_name in self.record_cache:
            cached_block_group = self.record_cache[block_group_name]
            census_tract_fips = cached_block_group['fips'][:-1]
            if census_tract_fips in census_tract_cache:
                resolved_census_tract_fips.add(census_tract_fips)

        census_tract_keys = list(census_tract_cache.keys())
        census_tract_index = 0
        while records_fetched < FETCHED_RECORDS_THRESHOLD and census_tract_index < len(census_tract_keys):
            census_tract_key = census_tract_keys[census_tract_index]
            census_tract = census_tract_cache[census_tract_key]
            census_tract_fips = census_tract['fips']
            if census_tract_fips not in resolved_census_tract_fips:
                block_group_url = f'{base_url}&within=1400000US{census_tract_fips}'
                response = requests.request('GET', block_group_url)
                response_content = json.loads(response.content.decode('cp437'))
                resolved_census_tract_fips.add(census_tract_fips)
                
                self.records.extend(response_content['response']['geos']['items'])
                records_fetched += 1
                progress(records_fetched, FETCHED_RECORDS_THRESHOLD, 'Records fetched')

            census_tract_index += 1
        self.fetch_complete = records_fetched < FETCHED_RECORDS_THRESHOLD


class BlockGroupPopulation(CensusPopulationResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_block_group
        ]

    @staticmethod
    def format_block_group(block_group):
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
        api_path = '?id=ACSDT5Y2020.B01003&g=010XX00US$1500000'
        self.fetch_resource(api_path, 'block_group')