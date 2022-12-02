from common.constants import entity_key
from entity.abstract import ResourceEntity
from common.utils import ensure_int

import datetime
import requests
import json
import csv
import io

URL = 'https://media.githubusercontent.com/media/uclalawcovid19behindbars/data' + \
      '/master/historical-data/historical_state_counts.csv'


def create_zero_based_record():
    return {
        'Residents.Confirmed': 0,
        'Staff.Confirmed': 0,
        'Residents.Deaths': 0,
        'Staff.Deaths': 0,
        'Residents.Tested': 0,
        'Residents.Initiated': 0,
        'Staff.Initiated': 0,
        'Residents.Completed': 0,
        'Staff.Completed': 0
    }


def create_cached_data(record):
    return {
        'Residents.Confirmed': ensure_int(record['Residents.Confirmed']),
        'Staff.Confirmed': ensure_int(record['Staff.Confirmed']),
        'Residents.Deaths': ensure_int(record['Residents.Deaths']),
        'Staff.Deaths': ensure_int(record['Staff.Deaths']),
        'Residents.Tested': ensure_int(record['Residents.Tested']),
        'Residents.Initiated': ensure_int(record['Residents.Initiated']),
        'Staff.Initiated': ensure_int(record['Staff.Initiated']),
        'Residents.Completed': ensure_int(record['Residents.Completed']),
        'Staff.Completed': ensure_int(record['Staff.Completed'])
    }


def assert_non_zero(record):
    if record['Residents.Confirmed'] < 0 or record['Staff.Confirmed'] < 0 or record['Residents.Deaths'] < 0 \
            or record['Staff.Deaths'] < 0 or record['Residents.Tested'] < 0 or record['Residents.Initiated'] < 0 \
            or record['Staff.Initiated'] < 0 or record['Residents.Completed'] < 0 or record['Staff.Completed'] < 0:
        print('Something Went Wrong!')


class CovidBehindBars(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_date,
            entity_key.census_us_state
        ]

    def get_calendar_date_id(self, record, field):
        calendar_date_cache = self.dependencies_cache[entity_key.calendar_date]
        return calendar_date_cache[record[field]]['id']

    def get_state_id(self, record, field):
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        return state_cache[record[field]]['id'] if record[field] in state_cache else None

    def __init__(self):
        super().__init__()

        self.table_name = 'covid_behind_bars'
        self.fields = [
            {'field': 'Date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'State', 'column': 'state_id', 'data': self.get_state_id},
            {'field': 'Residents.Confirmed', 'column': 'resident_cases'},
            {'field': 'Staff.Confirmed', 'column': 'staff_cases'},
            {'field': 'Residents.Deaths', 'column': 'resident_deaths'},
            {'field': 'Staff.Deaths', 'column': 'staff_deaths'},
            {'field': 'Residents.Tested', 'column': 'resident_tests'},
            # {'field': 'Residents.Active', 'column': 'resident_active'},
            # {'field': 'Staff.Active', 'column': 'staff_active'},
            {'field': 'Residents.Initiated', 'column': 'resident_administered_one_dose'},
            {'field': 'Staff.Initiated', 'column': 'staff_administered_one_dose'},
            {'field': 'Residents.Completed', 'column': 'resident_administered_two_dose'},
            {'field': 'Staff.Completed', 'column': 'staff_administered_two_dose'}
        ]
        self.cacheable_fields = ['calendar_date_id']

    def skip_record(self, record):
        return self.get_calendar_date_id(record, 'Date') in self.record_cache

    def ensure_bridged_gap(self, time_gap_elapse, covid_behind_bars_data, cached_data, state_name):
        time_gap_elapse_length = len(time_gap_elapse)
        if time_gap_elapse_length == 0:
            return

        while len(time_gap_elapse) > 0:
            time_gap_date = time_gap_elapse.pop(0)

            divisor = time_gap_elapse_length + 1
            residents_confirmed = ensure_int(covid_behind_bars_data['Residents.Confirmed'])
            staff_confirmed = ensure_int(covid_behind_bars_data['Staff.Confirmed'])
            residents_deaths = ensure_int(covid_behind_bars_data['Residents.Deaths'])
            staff_deaths = ensure_int(covid_behind_bars_data['Staff.Deaths'])
            residents_tested = ensure_int(covid_behind_bars_data['Residents.Tested'])
            residents_initiated = ensure_int(covid_behind_bars_data['Residents.Initiated'])
            staff_initiated = ensure_int(covid_behind_bars_data['Staff.Initiated'])
            residents_completed = ensure_int(covid_behind_bars_data['Residents.Completed'])
            staff_completed = ensure_int(covid_behind_bars_data['Staff.Completed'])

            self.records.append({
                'Date': time_gap_date,
                'State': state_name,
                'Residents.Confirmed': max((residents_confirmed - cached_data['Residents.Confirmed']) // divisor, 0),
                'Staff.Confirmed': max((staff_confirmed - cached_data['Staff.Confirmed']) // divisor, 0),
                'Residents.Deaths': max((residents_deaths - cached_data['Residents.Deaths']) // divisor, 0),
                'Staff.Deaths': max((staff_deaths - cached_data['Staff.Deaths']) // divisor, 0),
                'Residents.Tested': max((residents_tested - cached_data['Residents.Tested']) // divisor, 0),
                'Residents.Initiated': max((residents_initiated - cached_data['Residents.Initiated']) // divisor, 0),
                'Staff.Initiated': max((staff_initiated - cached_data['Staff.Initiated']) // divisor, 0),
                'Residents.Completed': max((residents_completed - cached_data['Residents.Completed']) // divisor, 0),
                'Staff.Completed': max((staff_completed - cached_data['Staff.Completed']) // divisor, 0)
            })

            assert_non_zero(self.records[-1])

    def fetch(self):
        state_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=100&id=4'
        response_content = json.loads(requests.request('GET', state_url).content)
        list_of_states = response_content['response']['geos']['items']

        response = requests.request('GET', URL)
        response_content = response.content.decode('utf-8')
        raw_data = list(csv.DictReader(io.StringIO(response_content)))

        state_cache = self.dependencies_cache[entity_key.census_us_state]
        covid_behind_bars_data_by_state = {}

        for state in list_of_states:
            state_name = state['name']
            covid_behind_bars_data_by_state[state_name] = {} if state_name in state_cache else None

        for data in raw_data:
            iso_date = data['Date']
            state = data['State']

            if state in state_cache:
                state = state_cache[state]

                covid_behind_bars_data_by_state[state['name']][iso_date] = data

        self.records = []
        self.updates = []
        current_date = datetime.date(2020, 1, 1)
        current_day = datetime.date.today()

        time_gap_elapse_by_state = {}
        cached_state_data = {}
        while current_date <= current_day:
            iso_date_key = str(current_date)

            for state in list_of_states:
                state_name = state['name']
                if state_name in state_cache and state_name in covid_behind_bars_data_by_state:

                    if iso_date_key in covid_behind_bars_data_by_state[state_name]:
                        covid_behind_bars_data = covid_behind_bars_data_by_state[state_name][iso_date_key]

                        if state_name not in cached_state_data:
                            cached_state_data[state_name] = create_zero_based_record()

                        if state_name not in time_gap_elapse_by_state:
                            time_gap_elapse_by_state[state_name] = []
                        else:
                            self.ensure_bridged_gap(
                                time_gap_elapse_by_state[state_name],
                                covid_behind_bars_data,
                                cached_state_data[state_name],
                                state_name
                            )

                        record = {'Date': iso_date_key, 'State': state_name}
                        residents_confirmed = ensure_int(covid_behind_bars_data['Residents.Confirmed'])
                        staff_confirmed = ensure_int(covid_behind_bars_data['Staff.Confirmed'])
                        residents_deaths = ensure_int(covid_behind_bars_data['Residents.Deaths'])
                        staff_deaths = ensure_int(covid_behind_bars_data['Staff.Deaths'])
                        residents_tested = ensure_int(covid_behind_bars_data['Residents.Tested'])
                        residents_initiated = ensure_int(covid_behind_bars_data['Residents.Initiated'])
                        staff_initiated = ensure_int(covid_behind_bars_data['Staff.Initiated'])
                        residents_completed = ensure_int(covid_behind_bars_data['Residents.Completed'])
                        staff_completed = ensure_int(covid_behind_bars_data['Staff.Completed'])

                        record['Residents.Confirmed'] = max(residents_confirmed - cached_state_data[state_name]['Residents.Confirmed'], 0)
                        record['Staff.Confirmed'] = max(staff_confirmed - cached_state_data[state_name]['Staff.Confirmed'], 0)
                        record['Residents.Deaths'] = max(residents_deaths - cached_state_data[state_name]['Residents.Deaths'], 0)
                        record['Staff.Deaths'] = max(staff_deaths - cached_state_data[state_name]['Staff.Deaths'], 0)
                        record['Residents.Tested'] = max(residents_tested - cached_state_data[state_name]['Residents.Tested'], 0)
                        record['Residents.Initiated'] = max(residents_initiated - cached_state_data[state_name]['Residents.Initiated'], 0)
                        record['Staff.Initiated'] = max(staff_initiated - cached_state_data[state_name]['Staff.Initiated'], 0)
                        record['Residents.Completed'] = max(residents_completed - cached_state_data[state_name]['Residents.Completed'], 0)
                        record['Staff.Completed'] = max(staff_completed - cached_state_data[state_name]['Staff.Completed'], 0)

                        assert_non_zero(record)

                        cached_state_data[state_name] = create_cached_data(covid_behind_bars_data)
                        self.records.append(record)

                    elif state_name not in cached_state_data:
                        zero_based_record = create_zero_based_record()
                        zero_based_record['Date'] = iso_date_key
                        zero_based_record['State'] = state_name

                        self.records.append(zero_based_record)

                    else:
                        time_gap_elapse_by_state[state_name].append(iso_date_key)

            current_date += datetime.timedelta(days=1)
