from common.constants import entity_key
from datetime import date, timedelta
from entity.abstract import ResourceEntity


class CalendarDate(ResourceEntity):

    @staticmethod
    def dependencies():
        return [
            entity_key.calendar_month,
            entity_key.calendar_day
        ]

    def __init__(self):
        super().__init__()

        self.table_name = 'calendar_date'
        self.fields = [
            {'field': 'date', 'column': 'date'},
            {'field': 'week_number', 'column': 'week_number'},
            {'field': 'calendar_day_id', 'column': 'calendar_day_id'},
            {'field': 'calendar_month_id', 'column': 'calendar_month_id'}
        ]
        self.record_cache = None
        self.records = None

    def load_cache(self):
        cachable_fields = ['date']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[str(record[field])] = record

    def get_cache(self):
        if self.record_cache is None:
            self.load_cache()

        return self.record_cache

    def skip_record(self, record):
        return record['date'] in self.record_cache if self.record_cache is not None else False

    def fetch(self):
        self.records = []

        calendar_month_cache = self.dependencies_cache[entity_key.calendar_month]
        calendar_day_cache = self.dependencies_cache[entity_key.calendar_day]
        current_date = date(2020, 1, 1)
        week_number = None

        while current_date < date.today():
            if current_date.month == 1 and current_date.day == 1:
                week_number = 1

            self.records.append({
                'date': str(current_date),
                'week_number': week_number,
                'calendar_month_id': calendar_month_cache[current_date.strftime('%B')]['id'],
                'calendar_day_id': calendar_day_cache[current_date.strftime('%A')]['id']
            })
            current_date += timedelta(days=1)
            if current_date.strftime('%A') == 'Sunday':
                week_number += 1


class CalendarMonth(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'calendar_month'
        self.record_cache = None
        self.records = []
        self.fields = [
            {'field': 'name', 'column': 'name'},
            {'field': 'short_name', 'column': 'short_name'},
            {'field': 'quarter', 'column': 'quarter'}
        ]

    def load_cache(self):
        cachable_fields = ['name', 'short_name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def get_cache(self):
        if self.record_cache is None:
            self.load_cache()

        return self.record_cache

    def skip_record(self, record):
        return record['name'] in self.record_cache if self.record_cache is not None else False

    def fetch(self):
        start, end = 1, 13
        name = 'name'
        short_name = 'short_name'
        quarter = 'quarter'

        this_year = date.today().year
        for month in range(start, end):
            d = date(this_year, month, 1)
            quotient = d.month // 3
            quarter_value = quotient if d.month % 3 == 0 else quotient + 1
            self.records.append({name: d.strftime('%B'), short_name: d.strftime('%b'), quarter: quarter_value})


class CalendarDay(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'calendar_day'
        self.record_cache = None
        self.records = []
        self.fields = [
            {'field': 'name'},
            {'field': 'short_name'}
        ]

    def load_cache(self):
        cachable_fields = ['name', 'short_name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def get_cache(self):
        if self.record_cache is None:
            self.load_cache()

        return self.record_cache

    def fetch(self):
        pass

    def save(self):
        pass


