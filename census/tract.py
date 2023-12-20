from common.constants import entity_key
from common.utils import progress, execute_threads
from common.http import send_request
from entity.abstract import ResourceEntity
from census.abstract import CensusPopulationResourceEntity

import threading


def get_census_tract_fips(record, field):
    [_, fips_code] = record[field].split('US')
    return fips_code


def format_census_tract_name(record, field):
    return record[field].replace(',', ';')


class CensusTract(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_county]

    def get_county_id(self, record, field):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_fips_code = record[field].split('US')[1][0:5]
        return county_entity.get_cached_value(county_fips_code)['id']

    def __init__(self):
        super().__init__()
        self.table_name = 'census_tract'

        self.fields = [
            {'field': 'name', 'column': 'name', 'data': format_census_tract_name},
            {'field': 'code', 'column': 'fips', 'data': get_census_tract_fips},
            {'field': 'code', 'column': 'county_id', 'data': self.get_county_id}
        ]

        self.cacheable_fields = ['fips', 'name', 'id']

    def skip_record(self, record):
        return 'code' in record and (
            '$' in record['code'] or get_census_tract_fips(record, 'code') in self.record_cache
        )

    def update_record(self, record):
        census_tract = self.get_cached_value(get_census_tract_fips(record, 'code'))
        return census_tract and census_tract['name'] != format_census_tract_name(record, 'name')

    def create_update_record(self, record):
        census_tract = self.record_cache[get_census_tract_fips(record, 'code')]
        record_id = census_tract['id']
        record_name = format_census_tract_name(record, 'name')
        return {'fields': ['name'], 'values': [record_name], 'clause': f'id = {record_id}'}

    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            for record in records:
                for field in self.cacheable_fields:
                    if field == 'name':
                        self.record_cache[format_census_tract_name(record, field)] = record
                    else:
                        self.record_cache[str(record[field])] = record

    def async_fetch(self, county_fips, shared_reference):
        base_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=6&showComponents=false'
        census_tract_url = f'{base_url}&within=050XX00US{county_fips}'
        response_content = send_request('GET', census_tract_url, 5, 2, encoding='utf-8')

        self.records.extend(response_content['response']['geos']['items'])
        shared_reference['records_fetched'] += 1
        progress(shared_reference['records_fetched'], shared_reference['record_count'], 'Records Fetched')

    def fetch(self):
        county_cache = self.dependencies_map[entity_key.census_us_county].record_cache
        resolved_county_fips = set()

        self.records = []
        self.updates = []
        record_count = len(county_cache)

        shared_reference = {'records_fetched': 0, 'record_count': record_count}
        thread_pool_limit = 25
        threads = []
        for county_key in county_cache:
            county = county_cache[county_key]
            county_fips = county['fips']
            if county_fips not in resolved_county_fips:
                args = (county_fips, shared_reference)
                threads.append(threading.Thread(target=self.async_fetch, args=args, name=county_fips))
                resolved_county_fips.add(county_fips)

                if len(threads) >= thread_pool_limit:
                    execute_threads(threads)

        execute_threads(threads)


class TractPopulation(CensusPopulationResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_tract]

    def get_tract_id(self, record, field):
        tract_entity = self.dependencies_map[entity_key.census_tract]
        tract = tract_entity.get_cached_value(format_census_tract_name(record, field))

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
        census_tract_id = self.get_tract_id(record, 'census_tract')
        return census_tract_id and str(census_tract_id) in self.record_cache

    def fetch(self):
        api_path = '?id=ACSDT5Y2020.B01003&g=010XX00US$1400000'
        self.fetch_resource(api_path, 'census_tract')
