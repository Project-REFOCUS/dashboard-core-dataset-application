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
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_fips_code = record[field].split('US')[1][0:5]
        return county_entity.get_cached_value(county_fips_code)['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'city'
        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'code', 'column': 'county_id', 'data': self.get_county_id}
        ]
        self.cacheable_fields = ['name']

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
            county_entity = self.dependencies_map[entity_key.census_us_county]
            county = county_entity.get_cached_value(county_cache_key)

            if county is not None:
                self.record_cache[record['code']] = county

        return county

    def skip_record(self, record):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_fips_code = record['code'].split('US')[1][0:5]
        if self.get_cached_value(record['name']) is None:
            county = county_entity.get_cached_value(county_fips_code)
            if county is None:
                self.search_county_for_city(record)
        else:
            county = None

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


class CityPopulation(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_city]

    def get_city_id(self, record, field):
        city_entity = self.dependencies_map[entity_key.census_us_city]
        return city_entity.get_cached_value(record[field])['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'city_population_2020'
        self.fields = [
            {'field': 'population', 'column': 'population'},
            {'field': 'city', 'column': 'city_id', 'data': self.get_city_id}
        ]
        self.cacheable_fields = ['city_id']

    def skip_record(self, record):
        city_entity = self.dependencies_map[entity_key.census_us_city]
        city = city_entity.get_cached_value(record['city'])
        return record['city'] in ignored_cities or city is None or self.get_cached_value(city['id']) is not None

    def fetch(self):
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=100&id=4'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        self.records = []
        topic = 'Population%20Total'
        data_id = 'ACSDT5Y2020.B01003'
        global_state_code = '$1600000'
        base_url = 'https://data.census.gov/api/access/data/table'

        for state in list_of_states:
            state_name, state_code = state['name'], state['code']
            if '$' in state_code:
                continue

            city_population_url = f'{base_url}?t={topic}&g={state_code}{global_state_code}&id={data_id}'
            response = requests.request('GET', city_population_url)
            if response.content is None or len(response.content) == 0:
                continue

            response_content = json.loads(response.content)
            population_data = response_content['response']['data']

            for data_index in range(1, len(population_data)):
                self.records.append({
                    'population': population_data[data_index][2],
                    'city': population_data[data_index][5]
                })
