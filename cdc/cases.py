from .abstract import CovidResourceEntity


class StateCases(CovidResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'state_cases'
        self.fields = [
            {'field': 'total_adm_all_covid_confirmed_past_7days', 'column': 'cases'},
            {'field': 'date', 'column': 'calendar_date_id', 'data': self.get_calendar_date_id},
            {'field': 'state', 'column': 'state_id', 'data': self.get_state_id}
        ]

    def update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        should_update = state_id in self.record_cache
        should_update = calendar_date_id in self.record_cache[state_id] if should_update else False
        return should_update and record['total_adm_all_covid_confirmed_past_7days'] != self.record_cache[state_id][calendar_date_id]['cases']

    def create_update_record(self, record):
        state_id = self.get_state_id(record, 'state')
        calendar_date_id = self.get_calendar_date_id(record, 'date')
        record_id = self.record_cache[state_id][calendar_date_id]['id']
        return {'fields': ['cases'], 'values': [record['total_adm_all_covid_confirmed_past_7days']], 'clause': f'id = {record_id}'}

    def fetch(self):
        self.fetch_resource('total_adm_all_covid_confirmed_past_7days')
