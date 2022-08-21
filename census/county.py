from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import json


def get_fips_code(record, *other):
    field = other[0]
    return record[field].split('US')[1].strip()


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
