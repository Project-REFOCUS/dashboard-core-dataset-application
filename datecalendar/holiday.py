from bs4 import BeautifulSoup
from common.constants import entity_key
from common.utils import from_string_to_date
from datetime import date
from entity.abstract import ResourceEntity

import requests


class CalendarHolidayType(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'holiday_type'
        self.record_cache = None
        self.records = []
        self.fields = []

    def load_cache(self):
        cachable_fields = ['name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def fetch(self):
        pass

    def save(self):
        pass


class CalendarHolidayDate(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_holiday,
            entity_key.calendar_date
        ]

    def __init__(self):
        super().__init__()

        self.table_name = 'holiday_calendar_date'
        self.fields = [
            {'field': 'holiday_id', 'column': 'holiday_id'},
            {'field': 'calendar_date_id', 'column': 'calendar_date_id'}
        ]
        self.cacheable_fields = ['calendar_date_id']

    def skip_record(self, record):
        return str(record['calendar_date_id']) in self.record_cache if self.record_cache is not None else False

    def fetch(self):
        url = 'https://www.officeholidays.com/countries/usa/'
        this_year = date.today().year
        year = 2020

        date_index = 1
        name_index = 2

        calendar_date_entity = self.dependencies_map[entity_key.calendar_date]
        holiday_entity = self.dependencies_map[entity_key.calendar_holiday]
        format_string = '%b %d %Y'

        self.records = []
        while year <= this_year:
            content = requests.request('GET', f'{url}{year}').content
            html = BeautifulSoup(content, features='html.parser')
            table_element = html.find('table', attrs={'class': 'country-table'})
            table_body_element = table_element.find('tbody')
            table_rows = table_body_element.find_all('tr')

            for row in table_rows:
                table_data_elements = row.find_all('td')
                name_data = table_data_elements[name_index].text
                date_data = table_data_elements[date_index].text
                holiday_date = from_string_to_date(f'{date_data} {year}', format_string)
                holiday_entity_record = holiday_entity.get_cached_value(name_data)

                if holiday_entity_record and holiday_date < date.today():
                    self.records.append({
                        'holiday_id': holiday_entity_record['id'],
                        'calendar_date_id': calendar_date_entity.get_cached_value(holiday_date)['id']
                    })

            year += 1


class CalendarHoliday(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_holiday_type,
            entity_key.calendar_date
        ]

    def __init__(self):
        super().__init__()

        self.table_name = 'holiday'
        self.record_cache = {}
        self.records = []
        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'holiday_type_id', 'column': 'holiday_type_id'},
            {'field': 'calendar_date_id', 'column': 'calendar_date_id'}
        ]
        self.cacheable_fields = ['name']

    def skip_record(self, record):
        return record['name'] in self.record_cache if self.record_cache is not None else False

    def fetch(self):
        pass

    def save(self):
        pass
