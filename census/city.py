from census.constants import ignored_cities, ignored_city_types, ignored_states, state_abbrev_map
from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import json
import time


class USCityType(ResourceEntity):

    def __init__(self):
        super().__init__()
        self.table_name = 'city_type'

        self.fields = []
        self.records = []
        self.updates = []

        self.cacheable_fields = ['name']


class USCity(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.census_us_city_type,
            entity_key.census_us_county,
            entity_key.census_us_state
        ]

    @staticmethod
    def create_cache_key(record):
        name, fips = record['name'], record['fips']
        return f'{name} ({fips})'

    @staticmethod
    def create_record_from_line(line):
        attributes = ['state', 'state_fips', 'fips', 'name', 'type', 'status', 'counties']
        values = line.strip().split('|')
        record = {}
        for (index, attribute) in enumerate(attributes):
            record[attribute] = values[index] if attribute != 'counties' else values[index].split(',')

        return record

    def get_city_name(self, record, field):
        state_entity = self.dependencies_map[entity_key.census_us_state]
        city_name = record[field]
        state_name = state_entity.get_cached_value(record['state'])['name']
        return f'{city_name}, {state_name}'

    def get_county_id(self, record, field):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_fips_code = record[field].split('US')[1][0:5]
        return county_entity.get_cached_value(county_fips_code)['id']

    def get_city_type_id(self, record, field):
        city_type_entity = self.dependencies_map[entity_key.census_us_city_type]
        return city_type_entity.get_cached_value(record[field])['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'city'
        self.fields = [
            {'field': 'name', 'column': 'name', 'data': self.get_city_name},
            {'field': 'fips'},
            {'field': 'type', 'column': 'city_type_id', 'data': self.get_city_type_id}
        ]
        self.cacheable_fields = []

    def load_cache(self):
        records = self.mysql_client.select(self.table_name)
        self.record_cache = {}
        for record in records:
            self.record_cache[USCity.create_cache_key(record)] = record
            self.record_cache[record['name']] = record

    def skip_record(self, record):
        state, city, fips = state_abbrev_map[record['state']], record['name'], record['fips']
        return f'{city}, {state} ({fips})' in self.record_cache or record['type'] in ignored_city_types or city in ignored_cities

    def fetch(self):
        # TODO: Think it might be better to get this from the state cache
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=18&showComponents=false'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        city_url = 'https://www2.census.gov/geo/docs/reference/codes/files'

        self.records = []
        for state in list_of_states:
            if state['name'] not in ignored_states:
                state_abbrev = state_abbrev_map[state['name']]
                state_fips_code = state['code'].split('US')[1]
                request_url = f'{city_url}/st{state_fips_code}_{state_abbrev.lower()}_places.txt'
                response = requests.request('GET', request_url)
                cities = response.content.decode('cp437').split('\n')

                for city in cities:
                    self.records.append(USCity.create_record_from_line(city))


class USCountyCities(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_city, entity_key.census_us_county]

    @staticmethod
    def create_records_from_line(line):
        attributes = ['state', 'state_fips', 'fips', 'name', 'type', 'status', 'counties']
        values = line.strip().split('|')
        record = {}
        for (index, attribute) in enumerate(attributes):
            record[attribute] = values[index]

        records = []
        for county in record['counties'].split(','):
            records.append({
                'state': record['state'],
                'state_fips': record['state_fips'],
                'fips': record['fips'],
                'name': record['name'],
                'type': record['type'],
                'county': county.strip()
            })

        return records

    def derive_county_from_census(self, record):
        state_fips, state_abbrev = record['state_fips'], record['state'].lower()
        url = f'https://www2.census.gov/geo/docs/reference/codes/files/st{state_fips}_{state_abbrev}_cou.txt'
        response = requests.request('GET', url)
        if response.status_code != 200:
            return None

        content_lines = response.content.decode('cp437').split('\n')
        derived_records_map = {}
        for line in content_lines:
            if line:
                attributes = ['state', 'state_fips', 'county_fips', 'name']
                values = line.strip().split(',')
                derived_record = {}
                for (index, attribute) in enumerate(attributes):
                    derived_record[attribute] = values[index]

                derived_records_map[derived_record['name']] = derived_record

        derived_county = None
        if record['county'] in derived_records_map:
            county_entity = self.dependencies_map[entity_key.census_us_county]
            derived_record = derived_records_map[record['county']]
            county_fips = derived_record['county_fips']
            state_fips = record['state_fips']
            derived_county = county_entity.get_cached_value(f'{state_fips}{county_fips}')

        return derived_county

    def get_county_id(self, *args):
        record = args[0]
        state = state_abbrev_map[record['state']]
        # Edge case where characters are being decoded differently
        county = record['county'].replace('±', 'ñ').replace('φ', 'í').replace('≤', 'ó').replace('ß', 'á').replace('ⁿ', 'ü')
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_object = county_entity.get_cached_value(f'{county}, {state}')
        if county_object is None:
            county_object = self.derive_county_from_census(record)

        return county_object['id']

    def get_city_id(self, *args):
        record = args[0]
        city_entity = self.dependencies_map[entity_key.census_us_city]
        city, state, fips = record['name'], state_abbrev_map[record['state']], record['fips']
        city_cache_key = f'{city}, {state} ({fips})'
        return city_entity.get_cached_value(city_cache_key)['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'county_cities'
        self.fields = [
            {'field': 'name', 'column': 'county_id', 'data': self.get_county_id},
            {'field': 'name', 'column': 'city_id', 'data': self.get_city_id}
        ]
        self.cacheable_fields = ['city_id']

    def skip_record(self, record):
        city_entity = self.dependencies_map[entity_key.census_us_city]
        city, state, fips = record['name'], state_abbrev_map[record['state']], record['fips']
        cache_key = f'{city}, {state} ({fips})'
        city_object = city_entity.get_cached_value(cache_key)
        return city_object is not None and str(city_object['id']) in self.record_cache or (record['type'] in ignored_city_types or city in ignored_cities)

    def fetch(self):
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=18&showComponents=false'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        city_url = 'https://www2.census.gov/geo/docs/reference/codes/files'

        self.records = []
        for state in list_of_states:
            if state['name'] not in ignored_states:
                state_abbrev = state_abbrev_map[state['name']]
                state_fips_code = state['code'].split('US')[1]
                request_url = f'{city_url}/st{state_fips_code}_{state_abbrev.lower()}_places.txt'
                response = requests.request('GET', request_url)
                cities = response.content.decode('cp437').split('\n')

                for city in cities:
                    self.records.extend(USCountyCities.create_records_from_line(city))


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
            if state_name in ignored_states:
                continue

            city_population_url = f'{base_url}?t={topic}&g={state_code}{global_state_code}&id={data_id}'
            response = requests.request('GET', city_population_url)
            retries = 0

            while response.content == 500 and retries < 3:
                time.sleep(2)

                response = requests.request('GET', city_population_url)
                retries += 1

            if response.status_code != 200 or response.content is None or len(response.content) == 0:
                continue

            response_content = json.loads(response.content)
            population_data = response_content['response']['data']

            for data_index in range(1, len(population_data)):
                self.records.append({
                    'population': population_data[data_index][2],
                    'city': population_data[data_index][5]
                })
