from common.constants import entity_key
from datetime import datetime
from entity.abstract import ResourceEntity

import requests
import json


def ensure_not_none(record, field, record_cache):
    return record[field] if record[field] is not None else 0


class StateTests(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_date,
            entity_key.census_us_state
        ]

    def get_calendar_date_id(self, record, field):
        calendar_date_cache = self.dependencies_cache[entity_key.calendar_date]
        iso_date = str(datetime.strptime(record[field], '%b %d %Y').date())
        return calendar_date_cache[iso_date]['id']

    def get_state_id(self, record, field):
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        return state_cache[record[field]]['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'state_tests'
        self.fields = [
            {'field': 'new_test_results_reported', 'column': 'tests', 'data': ensure_not_none},
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id}
        ]

    def load_cache(self):
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            if record['state_id'] not in self.record_cache:
                self.record_cache[record['state_id']] = {}

            self.record_cache[record['state_id']][record['calendar_date_id']] = record

    def skip_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        return state_id in self.record_cache and calendar_date_id in self.record_cache[state_id]

    def update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        should_update = state_id in self.record_cache
        should_update = calendar_date_id in self.record_cache[state_id] if should_update else False
        return should_update and ensure_not_none(record, 'new_test_results_reported', self.record_cache) != self.record_cache[state_id][calendar_date_id]['tests']

    def create_update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        record_id = self.record_cache[state_id][calendar_date_id]['id']
        return {'fields': ['tests'], 'values': [ensure_not_none(record, 'new_test_results_reported', self.record_cache)], 'clause': f'id = {record_id}'}

    def fetch(self):
        state_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=100&id=4'
        response_content = json.loads(requests.request('GET', state_url).content)
        list_of_states = response_content['response']['geos']['items']

        self.updates = []
        self.records = []
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        for state in list_of_states:
            if state['name'] in state_cache:
                state_abbrev = state_cache[state['name']]['short_name']
                cdc_url = f'https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=us_trend_by_{state_abbrev}'
                response = requests.request('GET', cdc_url)
                if response.status_code != 200:
                    continue

                response_content = json.loads(response.content)
                self.records.extend(response_content['us_trend_by_Geography'])
