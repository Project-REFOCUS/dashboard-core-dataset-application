from common.constants import entity_key
from entity.abstract import ResourceEntity
from datetime import date, timedelta

import re
import json
import requests

API_URL = 'https://data.cityofnewyork.us/resource/87fx-28ei.json' + \
    '?$select=`application_type`' + \
    '&$where=disposition_date >= \'{}\' and disposition_date <= \'{}\' &$limit=10000&$offset={}' + '&$order=disposition_date'


class MarketApplicationType(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'market_application_type'
        self.fields = [
            {'field': 'application_type', 'column': 'name'}

        ]
        self.cacheable_fields = ['name']
    
    def skip_record(self, record):
        return (self.record_cache and record['application_type'] in self.record_cache) or 'application_type' not in record

    def fetch(self):
        self.records = []
        application_type_set = set()

        today_date = date.today()
        begin_date = date(2020, 1, 1)

        continue_fetching = True
        offset = 0
        while continue_fetching:
            request_url = API_URL.format(
                begin_date,
                today_date,
                offset
            )
            
            records = json.loads(requests.request('GET', request_url).content.decode('utf-8'))

            for record in records:
                if 'application_type' in record:
                    if record['application_type'] not in application_type_set:
                        self.records.append(record)
                        application_type_set.add(record['application_type'])

            continue_fetching = len(records) == 1000
            offset += (1000 if continue_fetching else 0)