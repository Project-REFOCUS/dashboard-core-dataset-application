from common.constants import entity_key
from datetime import date, datetime
from entity.abstract import ResourceEntity
from common.utils import int_or_none, ensure_float

import requests
import csv
import io

URL = 'https://raw.githubusercontent.com/washingtonpost/data-police-shootings/master/fatal-police-shootings-data.csv'

race_ethnicity_mapping = {
    'A': 'Asian',
    'B': 'Black or African American',
    'H': 'Hispanic or Latino',
    'N': 'American Indian and Alaska Native',
    'O': 'Other',
    'W': 'White'
}


def get_gender_value(record, field):
    return 0 if record[field] == 'M' else 1


def get_mental_value(record, field):
    return 1 if record[field] else 0


def get_body_camera_value(record, field):
    return 1 if record[field] else 0


def get_age_value(record, field):
    return int_or_none(record[field])


def get_coordinate_value(record, field):
    return ensure_float(record[field])


class FatalShootings(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.census_us_city,
            entity_key.census_us_state,
            entity_key.calendar_date,
            entity_key.census_race_ethnicity
        ]

    def get_calendar_date_id(self, record, field):
        calendar_date_cache = self.dependencies_cache[entity_key.calendar_date]
        iso_date = record[field]
        return calendar_date_cache[iso_date]['id']

    def get_city_id(self, record, field):
        census_city_cache = self.dependencies_cache[entity_key.census_us_city]
        census_state_cache = self.dependencies_cache[entity_key.census_us_state]
        record_city_value = record[field]
        state_name = census_state_cache[record['state']]['name']
        city_key = f'{record_city_value} city, {state_name}'
        return census_city_cache[city_key]['id'] if city_key in census_city_cache else 0

    def get_race_ethnicity_id(self, record, field):
        census_race_ethnicity_cache = self.dependencies_cache[entity_key.census_race_ethnicity]
        race_ethnicity_name = 'Unknown'
        if record[field] in race_ethnicity_mapping:
            race_ethnicity_name = race_ethnicity_mapping[record[field]]

        return census_race_ethnicity_cache[race_ethnicity_name]['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'fatal_police_shootings'
        self.fields = [
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'id', 'column': 'public_id'},
            {'field': 'name'},
            {'field': 'manner_of_death'},
            {'field': 'armed'},
            {'field': 'age', 'column': 'age', 'data': get_age_value},
            {'field': 'gender', 'column': 'gender', 'data': get_gender_value},
            {'field': 'signs_of_mental_illness', 'column': 'mental', 'data': get_mental_value},
            {'field': 'threat_level'},
            {'field': 'body_camera', 'data': get_body_camera_value},
            {'field': 'city', 'column': 'city_id', 'data': self.get_city_id},
            {'field': 'race', 'column': 'race_ethnicity_id', 'data': self.get_race_ethnicity_id},
            {'field': 'longitude', 'data': get_coordinate_value},
            {'field': 'latitude', 'data': get_coordinate_value}
        ]

    def load_cache(self):
        cachable_fields = ['public_id']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[str(record[field])] = record

    def skip_record(self, record):
        return (record['id'] in self.record_cache if self.record_cache is not None else False)\
                or self.get_city_id(record, 'city') == 0

    def fetch(self):
        request = requests.request('GET', URL)
        raw_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')))

        self.records = []
        self.updates = []

        start_of_2020 = date(2020, 1, 1)
        records = list(raw_data)
        for record in records:
            record_date = datetime.fromisoformat(record['date']).date()
            if start_of_2020 <= record_date:
                self.records.append(record)

    def update_record(self, record):
        return False
