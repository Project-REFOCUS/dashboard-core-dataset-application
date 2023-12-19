from census.constants import ignored_states
from common import constants, http, utils
from entity.abstract import ResourceEntity
from common.constants import entity_key
from census.abstract import CensusPopulationResourceEntity
from common.performance import PerformanceLogger

import threading
import requests
import json

performance_logger = PerformanceLogger(__name__)

zipcode_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900' + \
              '&id=9&showComponents=false&within='


class USZipCode(ResourceEntity):

    @staticmethod
    def dependencies():
        return [constants.entity_key.census_us_state]

    def __init__(self):
        super().__init__()
        self.table_name = 'zipcode'

        self.fields = [
            {'field': 'name', 'column': 'value'}
        ]
        self.cacheable_fields = ['value']

    def skip_record(self, record):
        return record['name'] in self.record_cache

    def fetch(self):
        self.records = []

        state_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=18&showComponents=false'
        response_content = json.loads(requests.request('GET', state_url).content)
        list_of_states = response_content['response']['geos']['items']
        zipcode_set = set()

        for state in list_of_states:
            if 'collection' in state:
                continue

            state_code = state['code']
            response_content = http.get(f'{zipcode_url}{state_code}')
            list_of_zipcodes = response_content['response']['geos']['items']

            for zipcode in list_of_zipcodes:
                if 'collection' in zipcode:
                    continue
                zipcode_name = zipcode['name'].replace('ZCTA5 ', '').strip()
                if zipcode_name not in zipcode_set:
                    self.records.append({'name': zipcode_name})
                    zipcode_set.add(zipcode_name)


class USCityZipCodes(ResourceEntity):

    @staticmethod
    def dependencies():
        return [constants.entity_key.census_us_zipcode, constants.entity_key.census_us_county_city]

    @staticmethod
    def create_cache_key(city_id, zipcode_name):
        return f'C{city_id}_Z{zipcode_name}'

    def get_zipcode_id(self, record, field):
        zipcode_entity = self.dependencies_map[constants.entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(record[field])
        return zipcode['id'] if zipcode and 'id' in zipcode else None

    def __init__(self):
        super().__init__()

        self.table_name = 'city_zipcodes'
        self.fields = [
            {'field': 'name', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
            {'field': 'city_id'}
        ]
        self.cacheable_fields = ['city_id']
        self.states_consumed = set()
        self.list_of_states = []

    def skip_record(self, record):
        zipcode_id = self.get_zipcode_id(record, 'name')
        city_id = record['city_id']
        cached_record = self.get_cached_value(zipcode_id) if zipcode_id else None
        return not city_id or not zipcode_id or cached_record and city_id in cached_record

    def get_cached_value(self, key):
        return self.record_cache[key] if key in self.record_cache else None

    def load_cache(self):
        if not self.record_cache:
            self.record_cache = {}

        records = self.mysql_client.select(self.table_name)
        for record in records:
            zipcode_id = record['zipcode_id']
            city_id = record['city_id']

            if zipcode_id not in self.record_cache:
                self.record_cache[zipcode_id] = set()

            self.record_cache[zipcode_id].add(city_id)

    def fetch(self):
        if not self.list_of_states:
            url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=18&showComponents=false'
            response_content = json.loads(requests.request('GET', url).content)
            self.list_of_states = response_content['response']['geos']['items']

        zipcode_city_set = set()

        self.records = []
        states_length = len(self.list_of_states)
        states_index = 0
        needs_consuming = False
        while states_index < states_length and not needs_consuming:
            state = self.list_of_states[states_index]
            if state['name'] in ignored_states or 'collection' in state or state['name'] in self.states_consumed:
                self.states_consumed.add(state['name'])
                states_index += 1
            else:
                utils.debug('Consuming zipcodes for {}'.format(state['name']))
                needs_consuming = True

        state = self.list_of_states[states_index]
        state_name = state['name']
        self.states_consumed.add(state_name)
        [*_, state_code] = state['code'].split('US')
        fields = ['city.id id, city.fips fips']
        table = 'state,county,city,county_cities'
        where = f'state.id=county.state_id and state.name = \'{state_name}\'' \
                + ' and county.id=county_cities.county_id and city.id=county_cities.city_id'

        cities = self.mysql_client.select(table_name=table, fields=fields, where=where)
        shared_reference = {'zipcode_city_set': zipcode_city_set}
        thread_max_pool = 25
        threads = []
        for city in cities:
            args = (state_code, city, shared_reference)
            threads.append(threading.Thread(target=self.async_fetch, args=args, name=city['fips']))

            if len(threads) >= thread_max_pool:
                utils.execute_threads(threads)

        utils.execute_threads(threads)

    def async_fetch(self, state_code, city, shared_reference):
        city_fips = city['fips']
        zipcode_base_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900' + \
            '&id=9&showComponents=false&within=160XX00US'
        url = f'{zipcode_base_url}{state_code}{city_fips}'
        response_timer = performance_logger.start(url)
        response_content = http.get(url)
        response_timer.stop()
        zipcode_list = response_content['response']['geos']['items']

        for zipcode in zipcode_list:
            if 'collection' in zipcode:
                continue

            zipcode_name = zipcode['name'].replace('ZCTA5 ', '').strip()
            set_key = USCityZipCodes.create_cache_key(city['id'], zipcode_name)
            if set_key not in shared_reference['zipcode_city_set']:
                self.records.append({'name': zipcode_name, 'city_id': city['id']})
                shared_reference['zipcode_city_set'].add(set_key)

    def has_data(self):
        return len(self.list_of_states) > len(self.states_consumed)

    def after_save(self):
        super().after_save()
        # Fetch zipcodes from the next state
        self.fetch()


class ZipcodePopulation(CensusPopulationResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_zipcode]

    def get_zipcode_id(self, record, field):
        zipcode_value = record[field].split()[1]
        zipcode_entity = self.dependencies_map[entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(zipcode_value)
        return zipcode['id'] if zipcode else None
    
    def __init__(self):
        super().__init__()

        self.table_name = 'zipcode_population_2020'
        self.fields = [
            {'field': 'population'},
            {'field': 'zipcode', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
        ]
        self.cacheable_fields = ['zipcode_id']

    def skip_record(self, record):
        return self.record_cache and str(self.get_zipcode_id(record, 'zipcode')) in self.record_cache

    def fetch(self):
        api_path = '?id=ACSDT5Y2020.B01003&g=010XX00US$8600000'
        self.fetch_resource(api_path, 'zipcode')
