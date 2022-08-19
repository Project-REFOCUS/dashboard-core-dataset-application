from database.mysqldb import MysqlClient

import types


class ResourceEntity:

    @staticmethod
    def dependencies():
        return []

    def __init__(self):
        self.dependencies_cache = None
        self.record_cache = None
        self.table_name = None
        self.records = None
        self.fields = None

        self.mysql_client = MysqlClient()
        self.mysql_client.connect()

    def skip_record(self, record):
        return record in self.record_cache

    def set_dependencies_cache(self, key, dependency):
        if self.dependencies_cache is None:
            self.dependencies_cache = {}

        self.dependencies_cache[key] = dependency.get_cache()

    def load_cache(self):
        self.record_cache = {}

    def get_cache(self):
        return self.record_cache

    def fetch(self):
        self.records = []
        self.fields = []

    def save(self):
        if self.mysql_client.is_connected():
            self.mysql_client.start_transaction()

            # record_count = len(self.records)
            records_processed = 0

            # Used to cache values for additional calculations
            if self.record_cache is None:
                self.record_cache = {}

            for record in self.records:
                columns = []
                values = []

                if self.skip_record(record):
                    records_processed += 1
                    continue

                for field in self.fields:
                    if 'column' in field:
                        columns.append(field['column'])
                    elif 'field' in field:
                        columns.append(field['field'])

                    # Populating the values array
                    if 'data' in field:
                        if isinstance(field['data'], types.FunctionType):
                            values.append(field['data'](record, field['field'], self.record_cache))
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

            self.mysql_client.commit()

    def drop_cache(self):
        pass

    def __del__(self):
        self.mysql_client.close()
