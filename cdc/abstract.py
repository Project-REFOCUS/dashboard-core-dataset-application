from common.constants import entity_key, cache_id
from common.service import cached_request, remove_cached_request
from entity.abstract import ResourceEntity
from datetime import datetime, timedelta

import json


def ensure_not_none(value):
    return value if value is not None else 0


class CovidResourceEntity(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_date,
            entity_key.census_us_state
        ]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        return calendar_date_entity.get_cached_value(record[field])['id']

    def get_state_id(self, record, field):
        state_entity = self.dependencies_map[entity_key.census_us_state]
        return state_entity.get_cached_value(record[field])['id']

    def __init__(self):
        super().__init__()

    def load_cache(self):
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            if record['state_id'] not in self.record_cache:
                self.record_cache[record['state_id']] = {}

            self.record_cache[record['state_id']][record['calendar_date_id']] = record

    def skip_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        return state_id in self.record_cache and calendar_date_id in self.record_cache[state_id]

    def back_fill(self, value, prop):
        days_in_week = 7
        week_avg = int(value / days_in_week)
        for index in range(1, days_in_week):
            self.records[-(index + 1)][prop] = week_avg

        self.records[-1][prop] = week_avg + (value % days_in_week)

    def fetch_resource(self, resource):
        state_url = 'https://data.census.gov/api/explore/facets/geos/entityTypes?size=100&id=4'
        response_content = json.loads(cached_request(cache_id.list_of_states, 'GET', state_url).content)
        list_of_states = response_content['response']['geos']['items']

        self.updates = []
        self.records = []
        state_entity = self.dependencies_map[entity_key.census_us_state]
        records_by_state = {}
        for state in list_of_states:
            state_object = state_entity.get_cached_value(state['name'])
            if state_object is not None:
                state_abbrev = state_object['short_name']
                request_id = f'{cache_id.us_trend_by}_{state_abbrev}'
                cdc_url = f'https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id={request_id}'
                response = cached_request(request_id, 'GET', cdc_url)
                if response.status_code != 200:
                    remove_cached_request(request_id)
                    continue

                if state_abbrev not in records_by_state:
                    records_by_state[state_abbrev] = {}

                records_by_date = records_by_state[state_abbrev]
                response_content = json.loads(response.content)

                for record in response_content['us_trend_by_Geography']:
                    iso_date = str(datetime.strptime(record['date'], '%b %d %Y').date())
                    records_by_date[iso_date] = record

                current_date = datetime(2020, 1, 1).date()
                today = datetime.today().date()
                while current_date < today:
                    iso_key = str(current_date)
                    if iso_key not in records_by_date:
                        self.records.append({resource: 0, 'date': iso_key, 'state': state_abbrev})
                    else:
                        value = ensure_not_none(records_by_date[iso_key][resource])
                        self.records.append({resource: value, 'date': iso_key, 'state': state_abbrev})
                        self.back_fill(value, resource)

                    current_date += timedelta(days=1)
