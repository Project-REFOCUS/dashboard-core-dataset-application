from common.constants import entity_key
from entity.abstract import ResourceEntity
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


class FatalShootingsRaceEthnicity(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.police_fatal_shootings, entity_key.census_race_ethnicity]

    @staticmethod
    def get_class_name():
        return f'{__name__}.{__class__.__name__}'

    def get_fatal_police_shooting_id(self, record, field):
        fatal_police_shooting_entity = self.dependencies_map[entity_key.police_fatal_shootings]
        police_shooting_record = fatal_police_shooting_entity.get_cached_value(record[field])
        return police_shooting_record['id'] if police_shooting_record is not None else 0

    def get_race_ethnicity_id(self, record, field):
        race_ethnicity_entity = self.dependencies_map[entity_key.census_race_ethnicity]
        race_ethnicity_name = 'Unknown'
        if record[field] in race_ethnicity_mapping:
            race_ethnicity_name = race_ethnicity_mapping[record[field]]

        return race_ethnicity_entity.get_cached_value(race_ethnicity_name)['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'fatal_police_shootings_race_ethnicity'
        self.fields = [
            {'field': 'race', 'column': 'race_ethnicity_id', 'data': self.get_race_ethnicity_id},
            {'field': 'id', 'column': 'fatal_police_shootings_id', 'data': self.get_fatal_police_shooting_id}
        ]
        self.cacheable_fields = ['fatal_police_shootings_id']

    def fetch(self):
        self.records = []
        self.updates = []

        records = fetch_police_shooting_data()
        for record in records:
            races = record['race'].split(';')
            for race in races:
                self.records.append({'race': race, 'id': record['id']})

    def skip_record(self, record):
        return record['id'] in self.record_cache or self.get_fatal_police_shooting_id(record, 'id') == 0

    def should_fetch_data(self):
        return not ResourceEntity.should_skip_fetch(self.get_class_name())
