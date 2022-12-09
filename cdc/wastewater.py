from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import json

BASE_URL = 'https://data.cdc.gov/api/id/2ew6-ywp6.json?$select=`county_names`,`county_fips`,`population_served`,`date_start`,`date_end`,`ptc_15d`,`detect_prop_15d`,`percentile`&$order=`detect_prop_15d`+ASC&$limit=25000&$offset={}'


def get_int_value_or_none(record, field):
    return int(float(record[field])) if field in record else None


class WasteWater(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_date,
            entity_key.census_us_county
        ]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        return calendar_date_entity.get_cached_value(record[field])['id']

    def get_county_id(self, record, field):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        return county_entity.get_cached_value(record[field])['id']

    def __init__(self):
        super().__init__()

        self.table_name = 'wastewater_metrics'
        self.fields = [
            {'field': 'date_end', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'county_fips', 'column': 'county_id', 'data': self.get_county_id},
            {'field': 'percentile', 'data': get_int_value_or_none},
            {'field': 'ptc_15d', 'column': 'percentile_change_over_15_days', 'data': get_int_value_or_none},
            {'field': 'detect_prop_15d', 'column': 'detected_proportion_over_15_days', 'data': get_int_value_or_none}
        ]

    def skip_record(self, record):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        return 'county_fips' not in record or county_entity.get_cached_value(record['county_fips']) is None

    def fetch(self):
        offset = 0

        self.records = []
        self.updates = []

        response_content = json.loads(requests.request('GET', BASE_URL.format(offset)).content)
        while len(response_content) > 0:
            offset += len(response_content)
            self.records.extend(response_content)
            response_content = json.loads(requests.request('GET', BASE_URL.format(offset)).content)
