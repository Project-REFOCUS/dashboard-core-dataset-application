from common.constants import entity_key
from entity.abstract import ResourceEntity
from datetime import date, timedelta

import re
import json
import requests

API_URL = 'https://data.cityofnewyork.us/resource/87fx-28ei.json' + \
    '?$select=`market`,`bic_number`,`account_name`,`application_type`,`disposition_date`,`postcode`,`effective_date`,`expiration_date`' + \
    '&$where=disposition_date = \'{}\' &$limit=10000&$offset={}'

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

        tomorrows_date = date(date.today().year, date.today().month, date.today().day + 1)
        current_date = date(2020, 1, 1)

        while current_date < tomorrows_date:
            continue_fetching = True
            offset = 0
            while continue_fetching:
                request_url = API_URL.format(
                    current_date,
                    offset
                )
                records = json.loads(requests.request('GET', request_url).content.decode('utf-8'))

                for item in records:
                    if 'application_type' in item:
                        if item['application_type'] not in application_type_set:
                            self.records.append(item)
                            application_type_set.add(item['application_type'])

                continue_fetching = len(records) == 1000
                offset += (1000 if continue_fetching else 0)

            current_date += timedelta(days=1)