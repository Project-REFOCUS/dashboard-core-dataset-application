from common.constants import entity_key
from common.utils import progress
from entity.abstract import ResourceEntity

import requests
import json


def get_census_tract_fips(record, field):
    [_, fips_code] = record[field].split('US')
    return fips_code


class CensusTract(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.census_us_county]

    def get_county_id(self, record, field):
        county_entity = self.dependencies_map[entity_key.census_us_county]
        county_fips_code = record[field].split('US')[1][0:5]
        return county_entity.get_cached_value(county_fips_code)['id']

    def __init__(self):
        super().__init__()
        self.table_name = 'census_tract'

        self.fields = [
            {'field': 'name'},
            {'field': 'code', 'column': 'fips', 'data': get_census_tract_fips},
            {'field': 'code', 'column': 'county_id', 'data': self.get_county_id}
        ]

        self.cacheable_fields = ['fips']

    def skip_record(self, record):
        return 'code' in record and '$' in record['code'] \
            or 'fips' in record and record['fips'] in self.record_cache

    def fetch(self):
        base_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=99900&id=6&showComponents=false'
        county_cache = self.dependencies_map[entity_key.census_us_county].record_cache
        resolved_county_fips = set()

        self.records = []
        record_count = len(county_cache)
        records_fetched = 0
        for tract_name in self.record_cache:
            cached_tract = self.record_cache[tract_name]
            county_fips = cached_tract['fips'][0:5]
            if county_fips in county_cache:
                resolved_county_fips.add(county_fips)

        for county_key in county_cache:
            county = county_cache[county_key]
            county_fips = county['fips']
            if county_fips not in resolved_county_fips:
                census_tract_url = f'{base_url}&within=050XX00US{county_fips}'
                response = requests.request('GET', census_tract_url)
                response_content = json.loads(response.content.decode('cp437'))

                self.records.extend(response_content['response']['geos']['items'])
                resolved_county_fips.add(county_fips)

            records_fetched += 1
            progress(records_fetched, record_count, 'Records fetched')
