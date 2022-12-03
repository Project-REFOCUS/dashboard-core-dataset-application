from common.utils import progress
from database.mysqldb import MysqlClient

import types


class ResourceEntity:

    @staticmethod
    def dependencies():
        return []

    def __init__(self):
        self.dependencies_cache = None
        self.dependencies_map = None
        self.cacheable_fields = None
        self.record_cache = None
        self.table_name = None
        self.updates = None
        self.records = None
        self.fields = None

        self.mysql_client = MysqlClient()
        self.mysql_client.connect()

    def skip_record(self, record):
        return 'id' in record and record['id'] in self.record_cache

    def update_record(self, record):
        return '' in record and '' in self.record_cache

    def create_update_record(self, record):
        record_id = record['id']
        return {'fields': [record['field']], 'values': [record['value']], 'clause': f'id = {record_id}'}

    def set_dependencies_cache(self, key, dependency):
        if self.dependencies_cache is None:
            self.dependencies_cache = {}

        self.dependencies_cache[key] = dependency.get_cache()

    def set_dependencies(self, key, dependency):
        if self.dependencies_map is None:
            self.dependencies_map = {}

        self.dependencies_map[key] = dependency

    def load_cache(self):
        if self.cacheable_fields is not None:
            records = self.mysql_client.select(self.table_name)
            for record in records:
                if self.record_cache is None:
                    self.record_cache = {}

                for field in self.cacheable_fields:
                    self.record_cache[str(record[field])] = record

    def get_cached_value(self, key):
        cache_key = str(key)
        return self.record_cache[cache_key] if cache_key in self.record_cache else None

    def get_cache(self):
        if self.record_cache is None or len(self.record_cache) == 0:
            self.load_cache()

        return self.record_cache

    def fetch(self):
        self.updates = []
        self.records = []
        self.fields = []

    def save(self):
        if self.mysql_client.is_connected():
            self.mysql_client.start_transaction()

            record_count = len(self.records)
            records_processed = 0

            # Used to cache values for additional calculations
            if self.record_cache is None:
                self.record_cache = {}

            for record in self.records:
                columns = []
                values = []

                if self.update_record(record):
                    self.updates.append(self.create_update_record(record))
                    records_processed += 1
                    progress(records_processed, record_count)
                    continue

                if self.skip_record(record):
                    records_processed += 1
                    progress(records_processed, record_count)
                    continue

                for field in self.fields:
                    if 'column' in field:
                        columns.append(field['column'])
                    elif 'field' in field:
                        columns.append(field['field'])

                    # Populating the values array
                    if 'data' in field:
                        if isinstance(field['data'], types.FunctionType):
                            values.append(field['data'](record, field['field']))
                        elif isinstance(field['data'], types.MethodType):
                            values.append(field['data'](record, field['field']))
                        else:
                            values.append(record[field['data']])

                    elif 'field' in field:
                        if isinstance(field['field'], types.FunctionType):
                            values.append(field['field'](record, field['field'], self.record_cache))
                        elif isinstance(field['field'], types.MethodType):
                            values.append(field['field'](record, field['field']))
                        else:
                            values.append(record[field['field']])

                self.mysql_client.insert(self.table_name, columns, values)

                records_processed += 1
                progress(records_processed, record_count)

            self.mysql_client.commit()

    def has_updates(self):
        return self.updates is not None and len(self.updates) > 0

    def update(self):
        if self.mysql_client.is_connected():
            # TODO: Figure out why transaction won't work with update
            # self.mysql_client.start_transaction()

            record_count = len(self.updates)
            records_processed = 0

            for update in self.updates:
                columns = update['fields']
                values = update['values']

                self.mysql_client.update(self.table_name, columns, values, update['clause'])

                records_processed += 1
                progress(records_processed, record_count)

            # self.mysql_client.commit()

    def drop_cache(self):
        pass

    def __del__(self):
        self.mysql_client.close()
