from common.contants import entity_key
from entity.abstract import ResourceEntity
from datetime import datetime, timedelta

API_URL = 'https://data.cityofnewyork.us/resource/dsg6-ifza.json' + \
    '?$select=`centername`,`legalname`,`zipcode`,`status`,`dc_id`,`childcaretype`' + \
    '&$where=inspectiondate >= \'{}\' and inspectiondate < \'{}\'&$limit=1000&$offset={}'

class ChildCareCenter(ResourceEntity):

    @staticmethod
    def dependencies():
        return[
            entity_key.census_us_zipcode,
            entity_key.childcare_center_type
        ]

    def get_zipcode_id(self, record, field):
        zipcode_entity = self.dependencies_map[entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(record[field])
        return zipcode['id'] if zipcode else None

    def get_caretype_id(self, record, field):
        childcare_type_entity = self.dependencies_map[entity_key.childcare_center_type]
        return childcare_type_entity.get_cached_value(record[field])['id']
    
    def __init__(self):
        super().__init__()

        self.table_name = 'childcare_center'
        self.fields = [
            {'field': 'centername', 'column': 'center_name'},
            {'field': 'legalname', 'column': 'legal_name'},
            {'field': 'zipcode', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
            {'field': 'status'},
            {'field': 'dc_id', 'column': 'public_id'},
            {'field': 'childcaretype', 'column': 'childcare_type_id', 'data': self.get_caretype_id},
        ]
        self.cacheable_fields = ['public_id']

    def skip_record(self, record):
        return self.record_cache and record['dc_id'] in self.record_cache

    def fetch(self):
        self.records = []

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
                self.records.extend(records)
                continue_fetching = len(records) == 1000
                offset += (1000 if continue_fetching else 0)

            current_date += timedelta(days=1)