from .abstract import CovidResourceEntity, ResourceEntity
from common.constants import entity_key
from datetime import datetime

import requests


class StateDeaths(CovidResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'state_deaths'
        self.fields = [
            {'field': 'COVID_deaths_weekly', 'column': 'deaths'},
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id}
        ]

    def update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        should_update = state_id in self.record_cache
        should_update = calendar_date_id in self.record_cache[state_id] if should_update else False
        return should_update and record['COVID_deaths_weekly'] != self.record_cache[state_id][calendar_date_id]['deaths']

    def create_update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        record_id = self.record_cache[state_id][calendar_date_id]['id']
        return {'fields': ['deaths'], 'values': [record['COVID_deaths_weekly']], 'clause': f'id = {record_id}'}

    def fetch(self):
        self.fetch_resource('COVID_deaths_weekly')


class NycCountyDeaths(ResourceEntity):
    API_URL = 'https://health.data.ny.gov/resource/xymy-pny5.json?' + \
        '$where=as_of_date between \'{}\' and \'{}\'&$limit=1000&$offset={}&$order=as_of_date'

    @staticmethod
    def dependencies():
        return [entity_key.calendar_date, entity_key.census_us_county]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        value = str(datetime.strptime(record[field], '%Y-%m-%dT%H:%M:%S.%f').date())
        calendar_date = calendar_date_entity.get_cached_value(value)
        return calendar_date['id'] if calendar_date else None

    def get_county_id(self, record, field):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_name = record[field]
        key = f'{county_name} County, New York'
        county = county_entity.get_cached_value(key)
        return county['id'] if county else None

    def __init__(self):
        super().__init__()

        self.table_name = 'county_deaths'
        self.fields = [
            {'field': 'as_of_date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'geography', 'column': 'county_id', 'data': self.get_county_id},
            {'field': 'total_by_place_of_fatality', 'column': 'deaths'}
        ]

    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        cached_records = self.mysql_client.select(self.table_name)
        for record in cached_records:
            calendar_date_id = record['calendar_date_id']
            if calendar_date_id not in self.record_cache:
                self.record_cache[calendar_date_id] = {}

            self.record_cache[calendar_date_id][record['county_id']] = record

    def skip_record(self, record):
        calendar_date_id = self.get_calendar_date_id(record, 'as_of_date')
        county_id = self.get_county_id(record, 'geography')
        return calendar_date_id in self.record_cache and county_id in self.record_cache[calendar_date_id] or \
            self.get_county_id(record, 'geography') is None

    def fetch(self):

        self.records = []
        target_year = datetime.today().year + 1
        year = 2020
        offset = 0

        while year < target_year:
            continue_fetching = True

            while continue_fetching:
                start_date = str(datetime(year, 1, 1).date())
                end_date = str(datetime(year + 1, 1, 1).date())
                response = requests.request('GET', NycCountyDeaths.API_URL.format(start_date, end_date, offset))
                records = []
                if response.status_code == 200:
                    records = response.json()
                    self.records.extend(response.json())

                continue_fetching = True if records else False
                offset = offset + 1000 if continue_fetching else 0

            year += 1
