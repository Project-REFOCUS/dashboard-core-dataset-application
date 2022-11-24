from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import json


def get_fips_code(record, field):
    return record[field].split('US')[1].strip()


# TODO: Trace why Petersburg Census Area, Alaska does not show up as a county
class USCounty(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_state]

    def get_state_id(self, record, field):
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        state_name = record[field].split(',')[1].strip()
        return state_cache[state_name]['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'county'
        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'code', 'column': 'fips', 'data': get_fips_code},
            {'field': 'name', 'column': 'state_id', 'data': self.get_state_id}
        ]

    def load_cache(self):
        cachable_fields = ['name', 'fips']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def skip_record(self, record):
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        county_name, state_name = record['name'].split(',') if ',' in record['name'] else ['', '']
        return state_name.strip() not in state_cache or record['name'] in self.record_cache

    def fetch(self):
        url = f'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=5&showComponents=false'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        self.records = []
        for state in list_of_states:
            state_code = state['code']
            response_content = json.loads(requests.request('GET', f'{url}&within={state_code}').content)
            self.records.extend(response_content['response']['geos']['items'])


class CountyPopulation(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_county]

    def get_county_id(self, record, field):
        county_cache = self.dependencies_cache[entity_key.census_us_county]
        return county_cache[record[field]]['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'county_population_2020'
        self.fields = [
            {'field': 'population', 'column': 'population'},
            {'field': 'county', 'column': 'county_id', 'data': self.get_county_id}
        ]

    def load_cache(self):
        cacheable_fields = ['county_id']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cacheable_fields:
                self.record_cache[record[field]] = record

    def skip_record(self, record):
        county_cache = self.dependencies_cache[entity_key.census_us_county]
        return county_cache[record['county']]['id'] in self.record_cache

    def fetch(self):
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=100&id=4'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        self.records = []
        topic = 'Population%20Total'
        data_id = 'ACSDT5Y2020.B01003'
        global_state_code = '$0500000'
        base_url = 'https://data.census.gov/api/access/data/table'

        for state in list_of_states:
            state_code = state['code']
            if '$' in state_code:
                continue

            county_population_url = f'{base_url}?t={topic}&g={state_code}{global_state_code}&id={data_id}'
            response = requests.request('GET', county_population_url)
            if response.content is None or len(response.content) == 0:
                continue

            response_content = json.loads(response.content)
            population_data = response_content['response']['data']

            for data_index in range(1, len(population_data)):
                self.records.append({
                    'population': population_data[data_index][2],
                    'county': population_data[data_index][5]
                })
