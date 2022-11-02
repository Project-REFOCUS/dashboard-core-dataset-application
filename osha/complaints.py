from common.constants import entity_key
from entity.abstract import ResourceEntity

import requests
import openpyxl
import os

URL = 'https://www.osha.gov/sites/default/files/' + \
      'Closed_Federal_State_Plan_Valid_COVID-19_New_Complaints_0430_through_0715_2022.xlsx'

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
        calendar_date_cache = self.dependencies_cache[entity_key.calendar_date]
        return calendar_date_cache[str(record[field].date())]['id']

    # TODO: Need to create a more robust city name resolution algorithm to reliably capture more data sets
    def get_city_id(self, record, field):
        city_cache = self.dependencies_cache[entity_key.census_us_city]
        state_cache = self.dependencies_cache[entity_key.census_us_state]
        capitalized_city_name = record[field].capitalize()
        state_name = state_cache[record['Site State']]['name'] if record['Site State'] in state_cache else None
        city_cache_key = f'{capitalized_city_name} city, {state_name}'
        if city_cache_key not in city_cache:
            city_cache_key = f'{capitalized_city_name} village, {state_name}'

        return city_cache[city_cache_key]['id'] if city_cache_key in city_cache else None

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

    def load_cache(self):
        cacheable_fields = ['upa']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cacheable_fields:
                self.record_cache[record[field]] = record

    def skip_record(self, record):
        return record['UPA #'] in self.record_cache or self.get_city_id(record, 'Site City') is None

    def fetch(self):
        request = requests.request('GET', URL)

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
