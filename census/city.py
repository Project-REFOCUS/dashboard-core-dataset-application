from census.constants import ignored_cities
from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import json


class USCity(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_county]

    def get_county_id(self, record, field):
        county_cache = self.dependencies_cache[entity_key.census_us_county]
        county_fips_code = record[field].split('US')[1][0:5]
        return county_cache[county_fips_code]['id'] if county_fips_code in county_cache\
            else self.record_cache[record['code']]['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'city'
        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'code', 'column': 'county_id', 'data': self.get_county_id}
        ]

    def load_cache(self):
        cachable_fields = ['name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def search_county_for_city(self, record):
        county = None
        search_code = record['code']
        search_base_url = 'https://data.census.gov/api/explore/facets/geos/entities?size=99900&selSlv=155&slv=155'
        response_content = json.loads(requests.request('GET', f'{search_base_url}&within={search_code}').content)
        if len(response_content['response']['items']) > 0:
            name_data = response_content['response']['items'][0]['name'].split(',')
            county_name = name_data[0].replace('(part)', '').strip()
            state_name = name_data[2].strip()
            county_cache_key = f'{county_name}, {state_name}'
            county_cache = self.dependencies_cache[entity_key.census_us_county]
            county = county_cache[county_cache_key] if county_cache_key in county_cache else None

            if county is not None:
                self.record_cache[record['code']] = county

        return county

    def skip_record(self, record):
        county_cache = self.dependencies_cache[entity_key.census_us_county]
        county_fips_code = record['code'].split('US')[1][0:5]
        county = county_cache[county_fips_code] if county_fips_code in county_cache\
            else self.search_county_for_city(record)

        return county is None or record['name'] in self.record_cache or record['name'] in ignored_cities

    def fetch(self):
        url = f'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=18&showComponents=false'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        self.records = []
        for state in list_of_states:
            state_code = state['code']
            response_content = json.loads(requests.request('GET', f'{url}&within={state_code}').content)
            self.records.extend(response_content['response']['geos']['items'])
