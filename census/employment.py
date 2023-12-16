from common.constants import entity_key
from common.http import send_request
from common.utils import execute_threads
from entity.abstract import ResourceEntity

import threading


THREAD_POOL_LIMIT = 25


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

    def skip_record(self, record):
        return self.record_cache and record['census_tract_id'] in self.record_cache

    def fetch(self):
        self.records = []
        census_tracts = self.mysql_client.select('census_tract')
        census_tracts_count = len(census_tracts)
        census_tract_index = 0
        threads = []
        while census_tract_index < census_tracts_count:
            batch_records = []
            batch_size = 20
            while len(batch_records) < batch_size and census_tract_index < census_tracts_count:
                batch_records.append(census_tracts[census_tract_index])
                census_tract_index += 1

            threads.append(threading.Thread(target=self.async_fetch, args=(batch_records,)))
            if len(threads) >= THREAD_POOL_LIMIT:
                execute_threads(threads)

        # Execute the threads that may be remaining
        execute_threads(threads)

    def async_fetch(self, batch_records):
        employed_index = 685
        unemployed_index = 776
        fetch_url = f'{EmploymentStatus.API_URL}{build_url_query(batch_records)}'
        records = send_request('GET', fetch_url, 5, 2, encoding='utf-8')
        if records and 'response' in records:
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

