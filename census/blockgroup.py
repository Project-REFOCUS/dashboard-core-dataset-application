from common.constants import entity_key
from common.utils import progress
from common.http import send_request
from entity.abstract import ResourceEntity
from census.abstract import CensusPopulationResourceEntity

import threading

FETCHED_RECORDS_THRESHOLD = 10000
THREAD_POOL_LIMIT = 75


def get_block_group_fips(record, field):
    [_, fips_code] = record[field].split('US')
    return fips_code


def format_block_group_name(record, field):
    return record[field].replace(',', ';')


class BlockGroup(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_tract]

    def get_census_tract_id(self, record, field):
        census_tract_entity = self.dependencies_map[entity_key.census_tract]
        census_tract_fips_code = record[field].split('US')[1][:-1]
        return census_tract_entity.get_cached_value(census_tract_fips_code)['id']

    def __init__(self):
        super().__init__()
        self.table_name = 'block_group'
        self.record_cache = {}

        self.fields = [
            {'field': 'name', 'column': 'name', 'data': format_block_group_name},
            {'field': 'code', 'column': 'fips', 'data': get_block_group_fips},
            {'field': 'code', 'column': 'census_tract_id', 'data': self.get_census_tract_id}
        ]

        self.updates = []
        self.cacheable_fields = ['name', 'fips', 'id']
        self.resolved_census_tract_fips = set()

    def skip_record(self, record):
        return 'code' in record and (
            '$' in record['code'] or format_block_group_name(record, 'name') in self.record_cache
        )

    def update_record(self, record):
        block_group = self.get_cached_value(get_block_group_fips(record, 'code'))
        return block_group and block_group['name'] != format_block_group_name(record, 'name')

    def create_update_record(self, record):
        block_group = self.record_cache[get_block_group_fips(record, 'code')]
        record_id = block_group['id']
        record_name = format_block_group_name(record, 'name')
        return {'fields': ['name'], 'values': [record_name], 'clause': f'id = {record_id}'}

    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            for record in records:
                for field in self.cacheable_fields:
                    if field == 'name':
                        self.record_cache[format_block_group_name(record, field)] = record
                    else:
                        self.record_cache[str(record[field])] = record

    def fetch(self):
        census_tract_cache = self.dependencies_map[entity_key.census_tract].record_cache

        self.records = []

        census_tract_keys = list(census_tract_cache.keys())
        census_tract_index = 0
        threads_by_fips = {}
        thread_shared_reference = {'records_fetched': 0}
        while thread_shared_reference['records_fetched'] < FETCHED_RECORDS_THRESHOLD and census_tract_index < len(census_tract_keys):
            census_tract_key = census_tract_keys[census_tract_index]
            census_tract = census_tract_cache[census_tract_key]
            census_tract_fips = census_tract['fips']
            if census_tract_fips not in self.resolved_census_tract_fips:

                thread_args = (census_tract_fips, thread_shared_reference)
                n = census_tract_fips
                threads_by_fips[census_tract_fips] = threading.Thread(target=self.async_fetch, args=thread_args, name=n)
                self.resolved_census_tract_fips.add(census_tract_fips)
                threads_by_fips[census_tract_fips].start()

                threads = threads_by_fips.values()
                if len(threads_by_fips) >= THREAD_POOL_LIMIT:
                    for thread in threads:
                        thread.join()

                    threads_by_fips = {}

            census_tract_index += 1

        for thread in threads_by_fips.values():
            thread.join()

    def async_fetch(self, census_tract_fips, records_shared_reference):
        base_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=7&showComponents=false'
        block_group_url = f'{base_url}&within=1400000US{census_tract_fips}'
        response_content = send_request('GET', block_group_url, 5, 2, encoding='utf-8')

        self.records.extend(response_content['response']['geos']['items'])
        records_shared_reference['records_fetched'] += 1
        progress(records_shared_reference['records_fetched'], FETCHED_RECORDS_THRESHOLD, 'Records fetched')

    def after_save(self):
        self.load_cache()
        self.fetch()

    def after_update(self):
        self.updates = []


class BlockGroupPopulation(CensusPopulationResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_block_group]

    @staticmethod
    def format_block_group(block_group):
        return block_group.replace('ñ', 'n').lower()

    def get_block_group_id(self, record, field):
        block_group_entity = self.dependencies_map[entity_key.census_block_group]
        block_group = block_group_entity.get_cached_value(format_block_group_name(record, field))

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
        block_group_id = self.get_block_group_id(record, 'block_group')
        return block_group_id and str(block_group_id) in self.record_cache

    def fetch(self):
        api_path = '?id=ACSDT5Y2020.B01003&g=010XX00US$1500000'
        self.fetch_resource(api_path, 'block_group')
