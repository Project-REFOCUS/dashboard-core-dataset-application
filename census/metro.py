from entity.abstract import ResourceEntity
from common.constants import entity_key

import requests
import json

api_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?' \
          + 'size=1095&id=25&showComponents=false&slv=320&selSlv=320'


class USMetroArea(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_state]

    @staticmethod
    def get_class_name():
        return f'{__name__}.{__class__.__name__}'

    def get_state_id(self, record, field):
        state_entity = self.dependencies_map[entity_key.census_us_state]
        (metro_name, state_name) = record[field].split(';')
        state = state_entity.get_cached_value(state_name.strip())
        return state['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'metro_area'

        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'name', 'column': 'state_id', 'data': self.get_state_id}
        ]
        self.cacheable_fields = ['name']

    def skip_record(self, record):
        return record['name'] in self.record_cache

    def should_fetch_data(self):
        return not ResourceEntity.should_skip_fetch(self.get_class_name())

    def fetch(self):
        response_content = json.loads(requests.request('GET', api_url).content)
        self.records = response_content['response']['geos']['items']

