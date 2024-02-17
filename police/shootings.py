from common.constants import entity_key
from entity.abstract import ResourceEntity
from common.utils import int_or_none
from .utils import fetch_police_shooting_data

URL = 'https://raw.githubusercontent.com/washingtonpost/data-police-shootings/master/v2/fatal-police-shootings-data.csv'


race_ethnicity_mapping = {
    'A': 'Asian',
    'B': 'Black or African American',
    'H': 'Hispanic or Latino',
    'N': 'American Indian and Alaska Native',
    'O': 'Other',
    'W': 'White'
}


def get_gender_value(record, field):
    return 0 if record[field] == 'male' else 1


def get_mental_value(record, field):
    return 1 if record[field] else 0


def get_body_camera_value(record, field):
    return 1 if record[field] else 0


def get_age_value(record, field):
    return int_or_none(record[field])


class FatalShootings(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.census_us_city,
            entity_key.census_us_state,
            entity_key.calendar_date,
            entity_key.census_race_ethnicity
        ]

    @staticmethod
    def get_class_name():
        return f'{__name__}.{__class__.__name__}'

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        iso_date = record[field]
        return calendar_date_entity.get_cached_value(iso_date)['id']

    def get_city_id(self, record, field):
        city_entity = self.dependencies_map[entity_key.census_us_city]
        state_entity = self.dependencies_map[entity_key.census_us_state]
        record_city_value = record[field]
        state_name = state_entity.get_cached_value(record['state'])['name']
        city_key = f'{record_city_value} city, {state_name}'
        city = city_entity.get_cached_value(city_key)
        return city['id'] if city is not None else 0

    def get_race_ethnicity_id(self, record, field):
        race_ethnicity_entity = self.dependencies_map[entity_key.census_race_ethnicity]
        race_ethnicity_name = 'Unknown'
        if record[field] in race_ethnicity_mapping:
            race_ethnicity_name = race_ethnicity_mapping[record[field]]

        return race_ethnicity_entity.get_cached_value(race_ethnicity_name)['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'fatal_police_shootings'
        self.fields = [
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'id', 'column': 'public_id'},
            {'field': 'name'},
            {'field': 'armed_with', 'column': 'armed'},
            {'field': 'age', 'column': 'age', 'data': get_age_value},
            {'field': 'gender', 'column': 'gender', 'data': get_gender_value},
            {'field': 'was_mental_illness_related', 'column': 'mental', 'data': get_mental_value},
            {'field': 'threat_type', 'column': 'threat_level'},
            {'field': 'body_camera', 'data': get_body_camera_value},
            {'field': 'city', 'column': 'city_id', 'data': self.get_city_id},
        ]
        self.cacheable_fields = ['public_id']

    def fetch(self):
        self.records = fetch_police_shooting_data()
        self.updates = []

    def skip_record(self, record):
        return (record['id'] in self.record_cache if self.record_cache is not None else False)\
                or self.get_city_id(record, 'city') == 0

    def should_fetch_data(self):
        return not ResourceEntity.should_skip_fetch(self.get_class_name())

    def update_record(self, record):
        return False
