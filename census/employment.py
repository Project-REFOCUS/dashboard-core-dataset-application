from common.constants import entity_key
from entity.abstract import ResourceEntity
from json import JSONDecodeError

import requests
import json


def build_url_query(census_tracts):
    return ':'.join(['1400000US' + tract['fips'] for tract in census_tracts])


class EmploymentStatus(ResourceEntity):
    API_URL = 'https://data.census.gov/api/access/data/table?id=ACSDP5Y2021.DP03&g='

    @staticmethod
    def dependencies():
        return [entity_key.census_tract]

    def __init__(self):
        super().__init__()

        self.table_name = 'employment_status'
        self.fields = [
            {'field': 'census_tract_id'},
            {'field': 'employed'},
            {'field': 'unemployed'}
        ]
        self.cacheable_fields = ['census_tract_id']

    def fetch(self):
        self.records = []
        census_tracts = self.mysql_client.select('census_tract')
        census_tracts_count = len(census_tracts)
        census_tract_index = 0
        while census_tract_index < census_tracts_count:
            batch_records = []
            batch_size = 20
            while len(batch_records) < batch_size and census_tract_index < census_tracts_count:
                batch_records.append(census_tracts[census_tract_index])
                census_tract_index += 1

            # field_index = 349
            employed_index = 685
            unemployed_index = 776
            # print(f'Calling endpoint {EmploymentStatus.API_URL}{build_url_query(batch_records)}')
            response = requests.request('GET', f'{EmploymentStatus.API_URL}{build_url_query(batch_records)}')
            response_content = response.content.decode('utf-8')
            try:
                records = json.loads(response_content)
                batch_record_count = len(batch_records)
                batch_index = 0
                while batch_index < batch_record_count:
                    data = records['response']['data']
                    if batch_index + 1 < len(data):
                        self.records.append({
                            'census_tract_id': batch_records[batch_index]['id'],
                            'employed': int(data[batch_index + 1][employed_index]),
                            'unemployed': int(data[batch_index + 1][unemployed_index])
                        })
                    batch_index += 1
            except JSONDecodeError:
                print('Cannot decode response content')
                print(response_content)
                census_tract_index += 1
