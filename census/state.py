from census.constants import state_abbrev_map, ignored_states
from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import json


def get_state_abbrev(record, field):
    return state_abbrev_map[record[field]]


class USState(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'state'
        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'name', 'column': 'short_name', 'data': get_state_abbrev}
        ]

    def skip_record(self, record, *other):
        state_name = record['name']
        return state_name in ignored_states or state_name in self.record_cache or state_name not in state_abbrev_map

    def load_cache(self):
        cachable_fields = ['name', 'short_name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def fetch(self):
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?id=4&size=100'
        raw_content = json.loads(requests.request('GET', url).content)
        self.records = raw_content['response']['geos']['items']


class StatePopulation(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_state]

    def get_state_id(self, record, field):
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        return state_cache[record[field]]['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'state_population_2020'
        self.fields = [
            {'field': 'population', 'column': 'population'},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id}
        ]

    def load_cache(self):
        cacheable_fields = ['state_id']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cacheable_fields:
                self.record_cache[record[field]] = record

    def skip_record(self, record):
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        return state_cache[record['state']]['id'] in self.record_cache

    def fetch(self):
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=100&id=4'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        self.records = []
        topic = 'Population%20Total'
        data_id = 'ACSDT5Y2020.B01003'
        all_states_code = list_of_states[0]['code']
        population_url = f'https://data.census.gov/api/access/data/table?t={topic}&g={all_states_code}&id={data_id}'
        response_content = json.loads(requests.request('GET', population_url).content)
        population_data = response_content['response']['data']

        for data_index in range(1, len(population_data)):
            self.records.append({'population': population_data[data_index][2], 'state': population_data[data_index][5]})


class StatePopulationByRaceEthnicity(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.census_us_state,
            entity_key.census_race_ethnicity
        ]

    def get_state_id(self, record, field):
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        return state_cache[record[field]]['id']

    def get_race_ethnicity_id(self, record, field):
        race_ethnicity_cache = self.dependencies_cache[entity_key.census_race_ethnicity]
        return race_ethnicity_cache[record[field]]

    def __init__(self):
        super().__init__()

        self.table_name = 'state_population_2020_by_race_ethnicity'
        self.fields = [
            {'field': 'population', 'column': 'population'},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id},
            {'field': 'race_ethnicity', 'column': 'race_ethnicity_id'}
        ]
