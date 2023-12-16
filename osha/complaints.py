from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import openpyxl
import os


URL = 'https://www.osha.gov/sites/default/files/' + \
      'Closed_Federal_State_Plan_Valid_COVID-19_New_Complaints_0430_through_0715_2022.xlsx'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
}

COLUMN_CODES = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P']


def row_has_data(worksheet, index):
    has_data = False
    column_index = 0
    column_count = len(COLUMN_CODES)
    while not has_data and column_index < column_count:
        has_data = worksheet[f'{COLUMN_CODES[column_index]}{index}'].value is not None
        column_index += 1

    return has_data

class OshaClosedComplaints(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_date,
            entity_key.census_us_state,
            entity_key.census_us_city
        ]

    def get_calendar_date_id(self, record, field):
        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        return calendar_date_entity.get_cached_value(str(record[field].date()))['id']

    # TODO: Need to create a more robust city name resolution algorithm to reliably capture more data sets
    def get_city_id(self, record, field):
        city_entity = self.dependencies_map[entity_key.census_us_city]
        state_entity = self.dependencies_map[entity_key.census_us_state]
        capitalized_city_name = record[field].capitalize()
        state = state_entity.get_cached_value(record['Site State'])
        state_name = state['name'] if state is not None else state
        city_cache_key = f'{capitalized_city_name} city, {state_name}'
        city = city_entity.get_cached_value(city_cache_key)
        if city is None:
            city_cache_key = f'{capitalized_city_name} village, {state_name}'
            city = city_entity.get_cached_value(city_cache_key)

        return city['id'] if city is not None else city

    def __init__(self):
        super().__init__()

        self.table_name = 'osha_complaints'
        self.fields = [
            {'field': 'UPA #', 'column': 'upa'},
            {'field': 'Estab Name', 'column': 'establishment'},
            {'field': 'Site Address 1', 'column': 'address_1'},
            {'field': 'Site Address 2', 'column': 'address_2'},
            {'field': 'Site Zip', 'column': 'zipcode'},
            {'field': 'Receipt Type', 'column': 'type'},
            {'field': 'Formality', 'column': 'formality'},
            {'field': 'Hazard Desc & Location', 'column': 'description'},
            {'field': 'UPA Receipt Date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'Site City', 'column': 'city_id', 'data': self.get_city_id}
        ]
        self.cacheable_fields = ['upa']

    def skip_record(self, record):
        return record['UPA #'] in self.record_cache or self.get_city_id(record, 'Site City') is None

    def fetch(self):
        request = requests.get(URL, headers=HEADERS)

        temp_file = open('temp.xlsx', 'wb')
        temp_file.write(request.content)
        temp_file.close()

        workbook = openpyxl.load_workbook('temp.xlsx')
        worksheet = workbook[workbook.get_sheet_names()[0]]

        column_names = []
        for code in COLUMN_CODES:
            column_names.append(worksheet[f'{code}1'].value)

        self.records = []
        self.updates = []

        index = 2
        while row_has_data(worksheet, index):
            record = {}

            column_code_index = 0
            for column_name in column_names:
                record[column_name] = worksheet[f'{COLUMN_CODES[column_code_index]}{index}'].value
                column_code_index += 1

            self.records.append(record)
            index += 1

        os.remove('temp.xlsx')
