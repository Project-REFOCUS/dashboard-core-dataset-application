from common.constants import entity_key
from entity.abstract import ResourceEntity
from datetime import datetime

import requests


class NYCCountySnap(ResourceEntity):

    API_URL = 'https://data.ny.gov/api/id/dq6j-8u8z.json?' + \
        '$where=year={}&$limit=1000&$order=:id'

    @staticmethod
    def dependencies():
        return [entity_key.calendar_date, entity_key.census_us_county]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        month = record[field]
        year = record['year']
        date_key = f'{month} {year}'
        d = datetime.strptime(date_key, '%B %Y').date()
        return calendar_date_entity.get_cached_value(d.isoformat())['id']

    def get_county_id(self, record, field):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_name = record[field].replace('City', '').strip()
        key = f'{county_name} County, New York'
        county = county_entity.get_cached_value(key)
        return county['id'] if county else None

    def __init__(self):
        super().__init__()

        self.table_name = 'snap_data'
        self.fields = [
            {'field': 'month', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'district', 'column': 'county_id', 'data': self.get_county_id},
            {'field': 'total_snap_persons', 'column': 'total_persons'},
            {'field': 'total_snap_benefits', 'column': 'total_benefits'},
            {'field': 'total_snap_households', 'column': 'total_households'},
            {'field': 'temporary_assistance_snap_households', 'column': 'ta_households'},
            {'field': 'temporary_assistance_snap_persons', 'column': 'ta_persons'},
            {'field': 'temporary_assistance_snap_benefits', 'column': 'ta_benefits'},
            {'field': 'non_temporary_assistance_snap_households', 'column': 'non_ta_households'},
            {'field': 'non_temporary_assistance_snap_persons', 'column': 'non_ta_persons'},
            {'field': 'non_temporary_assistance_snap_benefits', 'column': 'non_ta_benefits'}
        ]

    def load_cache(self):
        if self.record_cache is None:
            self.record_cache = {}

        cached_records = self.mysql_client.select(self.table_name)
        for record in cached_records:
            calendar_date_id = record['calendar_date_id']
            if calendar_date_id not in self.record_cache:
                self.record_cache[calendar_date_id] = set()

            self.record_cache[calendar_date_id].add(record['county_id'])

    def skip_record(self, record):
        calendar_date_id = self.get_calendar_date_id(record, 'month')
        county_id = self.get_county_id(record, 'district')
        return calendar_date_id in self.record_cache and county_id in self.record_cache[calendar_date_id] or \
            county_id is None

    def fetch(self):
        self.records = []
        target_year = datetime.today().year + 1
        year = 2020

        while year < target_year:
            response = requests.request('GET', NYCCountySnap.API_URL.format(year))
            if response.status_code == 200:
                self.records.extend(response.json())

            year += 1
