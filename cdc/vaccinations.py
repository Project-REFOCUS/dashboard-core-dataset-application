from common.constants import entity_key
from common.utils import from_string_to_date
from entity.abstract import ResourceEntity

import datetime
import requests
import json
import csv
import io

URL = 'https://data.cdc.gov/api/views/unsk-b7fc/rows.csv?accessType=DOWNLOAD'


def create_zero_based_record():
    return {'distributed': 0, 'administered': 0, 'administered_one_dose': 0, 'administered_two_dose': 0}


def create_cached_data(record):
    return {
        'distributed': int(record['Distributed']),
        'administered': int(record['Administered']),
        'administered_one_dose': int(record['Administered_Dose1_Recip']),
        'administered_two_dose': int(record['Series_Complete_Yes'])
    }


def assert_non_zero(record):
    if record['distributed'] < 0 or record['administered'] < 0 or record['administered_one_dose'] < 0 or record['administered_two_dose'] < 0:
        print('Something Went Wrong!')


class StateVaccinations(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_date,
            entity_key.census_us_state
        ]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        return calendar_date_entity.get_cached_value(record[field])['id']

    def get_state_id(self, record, field):
        state_entity = self.dependencies_map[entity_key.census_us_state]
        return state_entity.get_cached_value(record[field])['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'state_vaccinations'
        self.fields = [
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id},
            {'field': 'distributed'},
            {'field': 'administered'},
            {'field': 'administered_one_dose'},
            {'field': 'administered_two_dose'}
        ]
        self.cacheable_fields = ['calendar_date_id']

    def skip_record(self, record):
        return self.get_calendar_date_id(record, 'date') in self.record_cache

    def ensure_bridged_gap(self, time_gap_elapse, vaccination_data, cached_data, state_name):
        time_gap_elapse_length = len(time_gap_elapse)
        if time_gap_elapse_length == 0:
            return

        while len(time_gap_elapse) > 0:
            time_gap_date = time_gap_elapse.pop(0)

            divisor = time_gap_elapse_length + 1
            distributed = int(vaccination_data['Distributed'])
            administered = int(vaccination_data['Administered'])
            administered_one = int(vaccination_data['Administered_Dose1_Recip'])
            administered_two = int(vaccination_data['Series_Complete_Yes'])

            self.records.append({
                'date': time_gap_date,
                'state': state_name,
                'distributed': max((distributed - cached_data['distributed']) // divisor, 0),
                'administered': max((administered - cached_data['administered']) // divisor, 0),
                'administered_one_dose': max((administered_one - cached_data['administered_one_dose']) // divisor, 0),
                'administered_two_dose': max((administered_two - cached_data['administered_two_dose']) // divisor, 0)
            })

            assert_non_zero(self.records[-1])

    def fetch(self):
        state_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=100&id=4'
        response_content = json.loads(requests.request('GET', state_url).content)
        list_of_states = response_content['response']['geos']['items']

        response = requests.request('GET', URL)
        response_content = response.content.decode('utf-8')
        vaccines_raw_data = list(csv.DictReader(io.StringIO(response_content)))

        state_entity = self.dependencies_map[entity_key.census_us_state]
        vaccination_data_by_state = {}

        for state in list_of_states:
            state_name = state['name']
            state_object = state_entity.get_cached_value(state_name)
            vaccination_data_by_state[state_name] = {} if state_object is not None else None

        for data in vaccines_raw_data:
            iso_date = str(from_string_to_date(data['Date'], '%m/%d/%Y'))
            location = data['Location']
            state = state_entity.get_cached_value(location)

            if state is not None:
                vaccination_data_by_state[state['name']][iso_date] = data

        self.records = []
        current_date = datetime.date(2020, 1, 1)
        current_day = datetime.date.today()

        time_gap_elapse_by_state = {}
        cached_state_data = {}
        while current_date <= current_day:
            iso_date_key = str(current_date)

            for state in list_of_states:
                state_name = state['name']
                state_object = state_entity.get_cached_value(state_name)
                if state_object is not None and state_name in vaccination_data_by_state:

                    if iso_date_key in vaccination_data_by_state[state_name]:
                        vaccination_data = vaccination_data_by_state[state_name][iso_date_key]

                        if state_name not in cached_state_data:
                            cached_state_data[state_name] = create_zero_based_record()

                        if state_name not in time_gap_elapse_by_state:
                            time_gap_elapse_by_state[state_name] = []
                        else:
                            self.ensure_bridged_gap(time_gap_elapse_by_state[state_name], vaccination_data, cached_state_data[state_name], state_name)

                        record = {'date': iso_date_key, 'state': state_name}
                        distributed = int(vaccination_data['Distributed'])
                        administered = int(vaccination_data['Administered'])
                        administered_one = int(vaccination_data['Administered_Dose1_Recip'])
                        administered_two = int(vaccination_data['Series_Complete_Yes'])

                        record['distributed'] = max(distributed - cached_state_data[state_name]['distributed'], 0)
                        record['administered'] = max(administered - cached_state_data[state_name]['administered'], 0)
                        record['administered_one_dose'] = max(administered_one - cached_state_data[state_name]['administered_one_dose'], 0)
                        record['administered_two_dose'] = max(administered_two - cached_state_data[state_name]['administered_two_dose'], 0)

                        assert_non_zero(record)

                        cached_state_data[state_name] = create_cached_data(vaccination_data)
                        self.records.append(record)

                    elif state_name not in cached_state_data:
                        zero_based_record = create_zero_based_record()
                        zero_based_record['date'] = iso_date_key
                        zero_based_record['state'] = state_name

                        self.records.append({
                            'date': iso_date_key,
                            'state': state_name,
                            'distributed': 0,
                            'administered': 0,
                            'administered_one_dose': 0,
                            'administered_two_dose': 0
                        })

                    else:
                        time_gap_elapse_by_state[state_name].append(iso_date_key)

            current_date += datetime.timedelta(days=1)
