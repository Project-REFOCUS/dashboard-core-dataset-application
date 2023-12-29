from common.constants import entity_key
from entity.abstract import ResourceEntity


import requests
import openpyxl
import zipfile
import csv
import io
import os


class EpaDataField(ResourceEntity):

    @staticmethod
    def dependencies():
        return []

    def __init__(self):
        super().__init__()

        self.table_name = 'ej_data_field'
        self.fields = [
            {'field': 'GBD Fieldname', 'column': 'name'},
            {'field': 'Description', 'column': 'description'}
        ]
        self.cacheable_fields = ['name']
        self.field_urls = [
            'https://gaftp.epa.gov/EJScreen/2020/2020_EJSCREEEN_columns-explained.xlsx',
            'https://gaftp.epa.gov/EJScreen/2021/2021_EJSCREEEN_columns-explained.xlsx',
            'https://gaftp.epa.gov/EJScreen/2022/2022_EJSCREEN_BG_Columns.xlsx',
            'https://gaftp.epa.gov/EJScreen/2023/2.22_September_UseMe/EJSCREEN_2023_BG_Columns.xlsx'
        ]
        self.column_codes = ['A', 'B', 'C']

    def skip_record(self, record):
        return record['GBD Fieldname'] in self.record_cache

    def row_has_data(self, worksheet, index):
        has_data = False
        column_index = 0
        column_count = len(self.column_codes)
        while not has_data and column_index < column_count:
            has_data = worksheet[f'{self.column_codes[column_index]}{index}'].value is not None
            column_index += 1

        return has_data

    def should_start_processing(self, worksheet, index):
        column_count = len(self.column_codes)
        columns_with_data = []

        for column_code in self.column_codes:
            if worksheet[f'{column_code}{index}'].value is not None:
                columns_with_data.append(column_code)

        return len(columns_with_data) == column_count

    def should_fetch_data(self):
        return not ResourceEntity.should_skip_fetch(__name__)

    def fetch(self):
        field_name_set = set()
        column_names = ['Column Number', 'GBD Fieldname', 'Description']
        years = [2020, 2021, 2022]
        year_index = 0

        self.records = []

        for url in self.field_urls:
            request = requests.request('GET', url)
            filename = f'workbook_{years[year_index]}.xlsx'
            year_index += 1

            xlsx_file = open(filename, 'wb')
            xlsx_file.write(request.content)
            xlsx_file.close()

            workbook = openpyxl.load_workbook(filename)
            worksheet = workbook[workbook.get_sheet_names()[0]]

            index = 3
            while self.row_has_data(worksheet, index):
                record = {}
                for column_index, column in enumerate(column_names):
                    record[column] = worksheet[f'{self.column_codes[column_index]}{index}'].value

                if record['GBD Fieldname'] not in field_name_set:
                    field_name_set.add(record['GBD Fieldname'])
                    self.records.append(record)

                index += 1

            os.remove(filename)


def get_public_id(record, field):
    year, fips = record['year'], record['ID']
    field, object_id = record['field'], record[field]
    return f'{field}{year}{fips}{object_id}'


def get_ej_data_value(record, field):
    return record[record[field]]


class EpaDataValue(ResourceEntity):

    @staticmethod
    def dependencies():
        return [entity_key.epa_ejscreen_field, entity_key.census_block_group]

    def get_ej_data_field(self, record, field):
        ej_data_field_entity = self.dependencies_map[entity_key.epa_ejscreen_field]
        ej_data_field = ej_data_field_entity.get_cached_value(record[field])
        return ej_data_field['id']

    def get_block_group_id(self, record, field):
        block_group_entity = self.dependencies_map[entity_key.census_block_group]
        block_group = block_group_entity.get_cached_value(record[field])
        return block_group['id'] if block_group else None

    def __init__(self):
        super().__init__()

        self.table_name = 'ej_data_value'
        self.fields = [
            {'field': 'OBJECTID', 'column': 'public_id', 'data': get_public_id},
            {'field': 'year'},
            {'field': 'field', 'column': 'value', 'data': get_ej_data_value},
            {'field': 'field', 'column': 'ej_data_field_id', 'data': self.get_ej_data_field},
            {'field': 'ID', 'column': 'block_group_id', 'data': self.get_block_group_id}
        ]
        self.cacheable_fields = ['public_id']
        self.field_urls = [
            'https://gaftp.epa.gov/EJScreen/2020/EJSCREEN_2020_StatePctile.csv.zip',
            'https://gaftp.epa.gov/EJScreen/2021/EJSCREEN_2021_StatePctile.csv.zip',
            'https://gaftp.epa.gov/EJScreen/2022/EJSCREEN_2022_StatePct_with_AS_CNMI_GU_VI.csv.zip'
        ]

    def skip_record(self, record):
        public_id = get_public_id(record, 'OBJECTID')
        data_value = get_ej_data_value(record, 'field')
        return self.get_cached_value(public_id) or not data_value or not self.get_block_group_id(record, 'ID')

    def load_cache(self):
        # Loading the cache is way too expensive for this module
        self.record_cache = None

    def get_cached_value(self, key):
        cached_value = self.mysql_client.select(self.table_name, where=f'public_id="{key}"')
        return cached_value[0] if cached_value else None

    def fetch(self):
        filename_set = {
            'EJSCREEN_2020_StatePctile.csv',
            'EJSCREEN_2021_StatePctile.csv',
            'EJSCREEN_2022_StatePct_with_AS_CNMI_GU_VI.csv'
        }
        ignored_fields = {'OBJECTID', 'ID', 'CITY_NAME', 'STATE_NAME', 'ST_ABBREV', 'REGION', 'Shape_Length', 'Shape_Area'}
        self.records = []
        year = 2020
        for url in self.field_urls:
            request = requests.request('GET', url)
            zipfile_object = zipfile.ZipFile(io.BytesIO(request.content), mode='r')
            for file in zipfile_object.filelist:
                if file.filename in filename_set:
                    csv_file_content = zipfile_object.open(file.filename)
                    extracted_data = csv.DictReader(io.StringIO(csv_file_content.read().decode('utf-8')))

                    data_set = list(extracted_data)
                    for data in data_set:

                        for field in extracted_data.fieldnames:
                            if field in ignored_fields:
                                continue

                            record = {'year': year, 'field': field}

                            # Initialize record with defaults
                            for default_field in ignored_fields:
                                record[default_field] = data[default_field] if default_field in data else None

                            record[field] = data[field]
                            self.records.append(record)

                    year += 1
