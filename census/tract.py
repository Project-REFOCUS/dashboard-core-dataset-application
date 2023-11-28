from common.constants import entity_key
from common.utils import progress
from entity.abstract import ResourceEntity
from census.abstract import CensusPopulationResourceEntity

import requests
import json


def get_census_tract_fips(record, field):
    [_, fips_code] = record[field].split('US')
    return fips_code


class CensusTract(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_county]
    
    @staticmethod
    def format_census_tract(subject_tract):
        return subject_tract.replace(';',',').replace('├▒','ñ').replace('├│','ó').replace('├¡','í').replace('├í','á').replace('├╝','ü').lower()

    def get_county_id(self, record, field):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_fips_code = record[field].split('US')[1][0:5]
        return county_entity.get_cached_value(county_fips_code)['id']

    def __init__(self):
        super().__init__()
        self.table_name = 'census_tract'

        self.fields = [
            {'field': 'name'},
            {'field': 'code', 'column': 'fips', 'data': get_census_tract_fips},
            {'field': 'code', 'column': 'county_id', 'data': self.get_county_id}
        ]

        self.cacheable_fields = ['fips','name','id']

    def skip_record(self, record):
        return 'code' in record and '$' in record['code'] \
            or 'fips' in record and record['fips'] in self.record_cache
    
    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            for record in records:
                for field in self.cacheable_fields:
                    if field == 'name':
                        self.record_cache[self.format_census_tract(str(record[field]))] = record
                    else:
                        self.record_cache[str(record[field])] = record

    def fetch(self):
        base_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=6&showComponents=false'
        county_cache = self.dependencies_map[entity_key.census_us_county].record_cache
        resolved_county_fips = set()

        self.records = []
        record_count = len(county_cache)
        records_fetched = 0
        for tract_name in self.record_cache:
            cached_tract = self.record_cache[tract_name]
            county_fips = cached_tract['fips'][0:5]
            if county_fips in county_cache:
                resolved_county_fips.add(county_fips)

        for county_key in county_cache:
            county = county_cache[county_key]
            county_fips = county['fips']
            if county_fips not in resolved_county_fips:
                census_tract_url = f'{base_url}&within=050XX00US{county_fips}'
                response = requests.request('GET', census_tract_url)
                response_content = json.loads(response.content.decode('cp437'))

                self.records.extend(response_content['response']['geos']['items'])
                resolved_county_fips.add(county_fips)

            records_fetched += 1
            progress(records_fetched, record_count, 'Records fetched')


class TractPopulation(CensusPopulationResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_tract
        ]

    @staticmethod
    def format_census_tract(subject_tract):
        return subject_tract.lower()

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
        api_path = '?id=ACSDT5Y2020.B01003&g=010XX00US$1400000'
        self.fetch_resource(api_path, 'census_tract')