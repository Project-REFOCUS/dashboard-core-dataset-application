from census.constants import state_abbrev_map
from common.constants import entity_key
from common.utils import ensure_float, debug, execute_threads
from common.logger import Logger
from datetime import datetime
from entity.abstract import ResourceEntity

import threading
import requests
import time
import math
import json
import csv
import io
import re

logger = Logger(__name__)

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


def get_coordinate_value(record, field):
    return ensure_float(record[field])


def diff(value1, value2):
    return math.ceil(value1) - math.ceil(value2)


def launch_backoff_toggle_thread(shared_reference):
    time.sleep(5)
    shared_reference['backoff'] = False


class RacismDeclarations(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_city, entity_key.calendar_date]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        iso_date = None
        if MM_DD_YYYY_PATTERN.match(record[field]) is not None:
            iso_date = str(datetime.strptime(record[field], '%m/%d/%Y').date())
        elif MM_DD_YY_PATTERN.match(record[field]) is not None:
            iso_date = str(datetime.strptime(record[field], '%m/%d/%y').date())

        calendar_date = calendar_date_entity.get_cached_value(iso_date)
        return calendar_date['id'] if calendar_date is not None else calendar_date

    def get_city_id(self, record, field):
        city_entity = self.dependencies_map[entity_key.census_us_city]
        address = record['address']
        if field not in address and 'town' not in address:
            return None

        city_name = address[field] if field in address else address['town']
        state_name = address['state'] if 'state' in address else state_abbrev_map[record['State']]
        sanitized_city_name = city_name.replace('City of', '').strip()
        city_cache_key = f'{sanitized_city_name} city, {state_name}'
        city = city_entity.get_cached_value(city_cache_key)
        city_id = city['id'] if city is not None else city
        if city_id is None:
            debug(f'{sanitized_city_name} was not found in city entity cache')

        return city_id

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
        self.cacheable_fields = ['entity_name']

    def skip_record(self, record):
        return record['Entity'] in self.record_cache or self.get_city_id(record, 'city') is None\
               or self.get_calendar_date_id(record, 'Date of Declaration') is None

    def fetch(self):
        request = requests.request('GET', URL)
        raw_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')), fieldnames=FIELDNAMES)

        self.records = []
        self.updates = []

        start_processing = False

        thread_max_limit = 15
        shared_reference = {}
        threads = []
        for record in raw_data:
            if not start_processing:
                start_processing = should_start_processing(record)
            else:
                thread_name = record.get('Entity')
                args = (record, shared_reference)
                threads.append(threading.Thread(target=self.async_fetch, args=args, name=thread_name))
                if len(threads) >= thread_max_limit:
                    execute_threads(threads)

        execute_threads(threads)

    def async_fetch(self, record, shared_reference):
        if shared_reference and shared_reference['backoff']:
            time.sleep(5)

        url = NOMINATIM_API_URL.format(record['Latitude'], record['Longitude'])
        response = requests.request('GET', url)
        if response.status_code == 429 and not shared_reference['backoff']:
            shared_reference['backoff'] = True
            logger.warning(f'{threading.current_thread().name} has encountered a 429. Backing off for 5 seconds')
            threading.Thread(target=launch_backoff_toggle_thread, args=(shared_reference,)).start()
            self.async_fetch(record, shared_reference)
        else:
            content = json.loads(response.content.decode('utf-8'))
            null_response = {'city': 'N/A', 'county': 'N/A', 'state': 'N/A'}
            record['address'] = content['address'] if 'address' in content else null_response
            self.records.append(record)
