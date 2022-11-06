from .abstract import CovidResourceEntity


def ensure_not_none(record, field, record_cache):
    return record[field] if record[field] is not None else 0


class StateTests(CovidResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'state_tests'
        self.fields = [
            {'field': '7_day_test_results_reported', 'column': 'tests', 'data': ensure_not_none},
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id}
        ]

    def skip_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        return state_id in self.record_cache and calendar_date_id in self.record_cache[state_id]

    def update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        should_update = state_id in self.record_cache
        should_update = calendar_date_id in self.record_cache[state_id] if should_update else False
        return should_update and ensure_not_none(record, '7_day_test_results_reported', self.record_cache) != self.record_cache[state_id][calendar_date_id]['tests']

    def create_update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        record_id = self.record_cache[state_id][calendar_date_id]['id']
        return {'fields': ['tests'], 'values': [ensure_not_none(record, '7_day_test_results_reported', self.record_cache)], 'clause': f'id = {record_id}'}

    def fetch(self):
        self.fetch_resource('7_day_test_results_reported')
