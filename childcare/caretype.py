from common.constants import entity_key
from entity.abstract import ResourceEntity
from datetime import datetime, timedelta

import json
import requests

API_URL = 'https://data.cityofnewyork.us/resource/dsg6-ifza.json' + \
    '?$select=`childcaretype`' + \
    '&$where=inspectiondate >= \'{}\' and inspectiondate < \'{}\'&$limit=10000&$offset={}'

class ChildCareType(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'childcare_type'
        self.fields = [
            {'field': 'childcaretype', 'column': 'name'}
        ]
        self.cacheable_fields = ['name']
    
    def skip_record(self, record):
        return self.record_cache and record['childcaretype'] in self.record_cache

    def fetch(self):
        self.records = []
        care_type_set = set()

        tomorrows_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day + 1)
        current_date = datetime(2020, 1, 1)

        while current_date < tomorrows_date:
            ending_date = current_date + timedelta(days=1)
            continue_fetching = True
            offset = 0
            while continue_fetching:
                request_url = API_URL.format(
                    current_date.isoformat(),
                    ending_date.isoformat(),
                    offset
                )
                records = json.loads(requests.request('GET', request_url).content.decode('utf-8'))
                continue_fetching = len(records) == 10000
                for item in records:
                    if item['childcaretype'] not in care_type_set:
                        self.records.append(item)
                        care_type_set.add(item['childcaretype'])
                
                offset += (10000 if continue_fetching else 0)

            current_date += timedelta(days=1)