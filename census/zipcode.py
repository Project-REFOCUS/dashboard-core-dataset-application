from census.constants import ignored_states
from common import constants, http
from entity.abstract import ResourceEntity

import requests
import json

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
        return [constants.entity_key.census_us_zipcode, constants.entity_key.census_us_city]

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

    def skip_record(self, record):
        zipcode_id = self.get_zipcode_id(record, 'name')
        city_id = record['city_id']
        cached_record = self.get_cached_value(city_id) if city_id else None
        return not city_id or not zipcode_id or cached_record and cached_record['zipcode_id'] == zipcode_id

    def fetch(self):
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=18&showComponents=false'
        response_content = json.loads(requests.request('GET', url).content)
        list_of_states = response_content['response']['geos']['items']

        zipcode_base_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900' + \
                           '&id=9&showComponents=false&within=160XX00US'
        zipcode_city_set = set()

        self.records = []
        for state in list_of_states:
            if state['name'] in ignored_states or 'collection' in state:
                continue

            state_name = state['name']
            [*_, state_code] = state['code'].split('US')
            fields = ['city.id id, city.fips fips']
            table = 'state,county,city,county_cities'
            where = f'state.id=county.state_id and state.name = \'{state_name}\'' \
                    + ' and county.id=county_cities.county_id and city.id=county_cities.city_id'

            cities = self.mysql_client.select(table_name=table, fields=fields, where=where)

            for city in cities:
                city_fips = city['fips']
                response_content = http.get(f'{zipcode_base_url}{state_code}{city_fips}')
                zipcode_list = response_content['response']['geos']['items']

                for zipcode in zipcode_list:
                    if 'collection' in zipcode:
                        continue

                    zipcode_name = zipcode['name'].replace('ZCTA5 ', '').strip()
                    set_key = USCityZipCodes.create_cache_key(city['id'], zipcode_name)
                    if set_key not in zipcode_city_set:
                        self.records.append({'name': zipcode_name, 'city_id': city['id']})
                        zipcode_city_set.add(set_key)
