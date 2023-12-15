from common.constants import entity_key
from common.http import send_request
from entity.abstract import ResourceEntity
from datetime import datetime, timedelta


def get_crash_timestamp(record, field):
    crash_date = str(datetime.strptime(record['crash_date'], '%Y-%m-%dT%H:%M:%S.%f').date())
    crash_datetime = datetime.strptime(f'{crash_date}T{record[field]}', '%Y-%m-%dT%H:%M')
    return crash_datetime.isoformat()


class NYCMotorVehicleCollisions(ResourceEntity):

    API_URL = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json' + \
              '?$select=`crash_date`,`crash_time`,`zip_code`,`collision_id`' + \
              '&$where=crash_date >= \'{}\' and crash_date < \'{}\'&$limit=1000&$offset={}'

    @staticmethod
    def dependencies():
        return [entity_key.calendar_date, entity_key.census_us_zipcode]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        crash_datetime = datetime.strptime(record[field], '%Y-%m-%dT%H:%M:%S.%f')
        calendar_date = calendar_date_entity.get_cached_value(crash_datetime.date())
        return calendar_date['id']

    def get_zipcode_id(self, record, field):
        zipcode_entity = self.dependencies_map[entity_key.census_us_zipcode]
        zipcode = zipcode_entity.get_cached_value(record[field])
        return zipcode['id'] if zipcode else None

    def __init__(self):
        super().__init__()

        self.table_name = 'motor_vehicle_collisions'
        self.fields = [
            {'field': 'crash_date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'zip_code', 'column': 'zipcode_id', 'data': self.get_zipcode_id},
            {'field': 'crash_time', 'column': 'crash_time', 'data': get_crash_timestamp},
            {'field': 'collision_id', 'column': 'public_id'}
        ]
        self.cacheable_fields = ['public_id']

    def skip_record(self, record):
        zipcode_id = self.get_zipcode_id(record, 'zip_code') if 'zip_code' in record else None
        return zipcode_id is None or self.record_cache and record['collision_id'] in self.record_cache

    def fetch(self):
        self.records = []
        target_date = datetime(datetime.today().year + 1, 1, 1)
        current_date = datetime(2020, 1, 1)

        while current_date < target_date:
            ending_date = current_date + timedelta(days=1)
            continue_fetching = True
            offset = 0
            while continue_fetching:
                request_url = NYCMotorVehicleCollisions.API_URL.format(
                    current_date.isoformat(),
                    ending_date.isoformat(),
                    offset
                )
                records = send_request('GET', request_url, 5, 2, encoding='utf-8')
                continue_fetching = len(records) == 1000
                self.records.extend(records)
                offset += (1000 if continue_fetching else 0)

            current_date += timedelta(days=1)
