from common.constants import entity_key
from entity.abstract import ResourceEntity
from datetime import datetime, timedelta

API_URL = 'https://data.cityofnewyork.us/resource/87fx-28ei.json' + \
    '?$select=`application_type`' + \
    '&$where=disposition_date = \'{}\' &$limit=1000&$offset={}'

class MarketApplicationType(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'market_application_type'
        self.fields = [
            {'field': 'application_type', 'column': 'name'}

        ]
        self.cacheable_fields = ['name']
    
    def skip_record(self, record):
        return self.record_cache and record['name'] in self.record_cache

    def fetch(self):
        self.records = []

        tomorrows_date = date(date.today().year, date.today().month, date.today().day + 1)
        current_date = date(2020, 1, 1)

        while current_date < tomorrows_date:
            continue_fetching = True
            offset = 0
            while continue_fetching:
                request_url = API_URL.format(
                    current_date.date(),
                    offset
                )
                records = json.loads(requests.request('GET', request_url).content.decode('utf-8'))
                self.records.extend(records)
                continue_fetching = len(records) == 1000
                offset += (1000 if continue_fetching else 0)

            current_date += timedelta(days=1)