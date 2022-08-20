from census.constants import state_abbrev_map, ignored_states
from entity.abstract import ResourceEntity

import requests
import json


def get_state_abbrev(record, field, record_cache):
    return state_abbrev_map[record[field]]


class USState(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'state'
        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'name', 'column': 'short_name', 'data': get_state_abbrev}
        ]

    def skip_record(self, record, *other):
        state_name = record['name']
        return state_name in ignored_states or state_name in self.record_cache or state_name not in state_abbrev_map

    def load_cache(self):
        cachable_fields = ['name', 'short_name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def fetch(self):
        url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?id=4&size=100'
        raw_content = json.loads(requests.request('GET', url).content)
        self.records = raw_content['response']['geos']['items']
