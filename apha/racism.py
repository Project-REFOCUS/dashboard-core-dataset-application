from common.constants import entity_key
from common.utils import ensure_float
from datetime import datetime
from entity.abstract import ResourceEntity

import requests
import math
import time
import json
import csv
import io
import re

NOMINATIM_API_URL = 'https://nominatim.openstreetmap.org/reverse?format=json&lat={}&lon={}'
URL = 'https://docs.google.com/spreadsheets/d/e' + \
      '/2PACX-1vRkLVhZFb2K0K9LWxxrZujKaGP1qcpsbZ9gCtAfM6eHGZuNa_qxBHwKpSPYfAiPSoMChphBJsMd4o7Z/pub' +\
      '?gid=1592883182&single=true&output=csv'
FIELDNAMES = [
    'State', 'Region', 'Address', 'Latitude', 'Longitude', 'Type', 'Sub-Type', 'Entity',
    'Political Affiliation', 'Declaration', 'Date of Declaration', 'Link', 'Notes'
]
MM_DD_YYYY_PATTERN = re.compile('\\d{1,2}/\\d{1,2}/\\d{4}')
MM_DD_YY_PATTERN = re.compile('\\d{1,2}/\\d{1,2}/\\d{2}')


def should_start_processing(record):
    start_processing = True
    for field_name in FIELDNAMES:
        start_processing = field_name in record and record[field_name] == field_name and start_processing

    return start_processing


def get_coordinate_value(record, field, record_cache):
    return ensure_float(record[field])


def diff(value1, value2):
    return math.ceil(value1) - math.ceil(value2)


class RacismDeclarations(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.census_us_city,
            entity_key.calendar_date
        ]

    def get_address_and_by_coordinates(self, latitude, longitude, retry=False):

        if retry:
            time.sleep(1)

        if self.last_api_call_time is None or diff(time.perf_counter(), self.last_api_call_time) > 1:
            request = requests.request('GET', NOMINATIM_API_URL.format(latitude, longitude))
            if request.status_code == 200:
                null_response = {'city': 'N/A', 'county': 'N/A', 'state': 'N/A'}
                response = json.loads(request.content.decode('utf-8'))
                self.last_api_call_time = time.perf_counter()
                return response['address'] if 'address' in response else null_response
            else:
                return self.get_address_and_by_coordinates(latitude, longitude, retry=True)
        else:
            return self.get_address_and_by_coordinates(latitude, longitude, retry=True)

    def get_calendar_date_id(self, record, field):
        calendar_date_cache = self.dependencies_cache[entity_key.calendar_date]
        iso_date = None
        if MM_DD_YYYY_PATTERN.match(record[field]) is not None:
            iso_date = str(datetime.strptime(record[field], '%m/%d/%Y').date())
        elif MM_DD_YY_PATTERN.match(record[field]) is not None:
            iso_date = str(datetime.strptime(record[field], '%m/%d/%y').date())

        return calendar_date_cache[iso_date]['id'] if iso_date in calendar_date_cache else None

    def get_city_id(self, record, field):
        city_cache = self.dependencies_cache[entity_key.census_us_city]
        address = self.get_address_and_by_coordinates(record['Latitude'], record['Longitude'])
        if field not in address and 'town' not in address:
            return None

        city_name = address[field] if field in address else address['town']
        state_name = address['state']
        city_cache_key = f'{city_name} city, {state_name}'
        return city_cache[city_cache_key]['id'] if city_cache_key in city_cache else None

    def __init__(self):
        super().__init__()

        self.table_name = 'apha_racism_declarations'
        self.last_api_call_time = None
        self.fields = [
            {'field': 'Date of Declaration', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'Longitude', 'column': 'longitude', 'data': get_coordinate_value},
            {'field': 'Latitude', 'column': 'latitude', 'data': get_coordinate_value},
            {'field': 'Sub-Type', 'column': 'entity_type'},
            {'field': 'Type', 'column': 'entity_geo'},
            {'field': 'Entity', 'column': 'entity_name'},
            {'field': 'Link', 'column': 'link_to_declaration'},
            {'field': 'Declaration', 'column': 'declaration'},
            {'field': 'city', 'column': 'city_id', 'data': self.get_city_id}
        ]

    def load_cache(self):
        cacheable_fields = ['entity_name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cacheable_fields:
                self.record_cache[str(record[field])] = record

    def skip_record(self, record):
        return record['Entity'] in self.record_cache or self.get_city_id(record, 'city') is None\
               or self.get_calendar_date_id(record, 'Date of Declaration') is None

    def fetch(self):
        request = requests.request('GET', URL)
        raw_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')), fieldnames=FIELDNAMES)

        self.records = []
        self.updates = []

        start_processing = False

        for record in raw_data:
            if not start_processing:
                start_processing = should_start_processing(record)
            else:
                self.records.append(record)