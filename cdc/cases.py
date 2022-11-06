from .abstract import CovidResourceEntity


class StateCases(CovidResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'state_cases'
        self.fields = [
            {'field': 'New_case', 'column': 'cases'},
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id}
        ]

    def update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        should_update = state_id in self.record_cache
        should_update = calendar_date_id in self.record_cache[state_id] if should_update else False
        return should_update and record['New_case'] != self.record_cache[state_id][calendar_date_id]['cases']

    def create_update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        record_id = self.record_cache[state_id][calendar_date_id]['id']
        return {'fields': ['cases'], 'values': [record['New_case']], 'clause': f'id = {record_id}'}

    def fetch(self):
        self.fetch_resource('New_case')
