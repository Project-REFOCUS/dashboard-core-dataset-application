from mysql.connector import connect
from common import utils

import os


SQL_MAX_LENGTH = 10000


def missing_env_var(var):
    print(f'Missing environment variable {var}')


def escape_quotes(value):
    return f'`{value}`'


def value_or_empty(value, prepended=''):
    return '{} {}'.format(prepended, value) if value is not None else ''


def generate_columns_string(columns):
    columns_string = ''
    delimiter = ''
    for column in columns:
        columns_string += delimiter
        columns_string += escape_quotes(column)
        delimiter = ', '

    return f'({columns_string})'


def generate_values_placeholders(length):
    values_placeholders = ''
    delimiter = ''
    iterator = 0
    while iterator < length:
        values_placeholders += delimiter
        values_placeholders += '%s'
        iterator += 1
        delimiter = ', '

    return f'({values_placeholders})'


def generate_values_string(values):
    values_string = ''
    delimiter = ''
    for value in values:
        values_string += delimiter
        if isinstance(value, str):
            replaced_value = value.replace('\'', '\\\'')
            values_string += f'\'{replaced_value}\''
        else:
            values_string += str(value)

        delimiter = ', '

    return f'({values_string})'


class MysqlClient:

    def __init__(self):
        self.cache = {}

        self.hostname = os.getenv('MYSQL_HOST')
        self.username = os.getenv('MYSQL_USER')
        self.password = os.getenv('MYSQL_PASS')
        self.name = os.getenv('MYSQL_NAME')
        self.port = os.getenv('MYSQL_PORT')

        self.connection = None
        self.cursor = None
        self.reset_cache()

    def connect(self):
        if self.hostname is None:
            missing_env_var('MYSQL_HOST')
        elif self.username is None:
            missing_env_var('MYSQL_USER')
        elif self.password is None:
            missing_env_var('MYSQL_PASS')
        elif self.name is None:
            missing_env_var('MYSQL_NAME')
        elif self.port is None:
            missing_env_var('MYSQL_PORT')
        elif self.is_connected():
            print('There is already an active connection with this client instance')
        else:
            self.connection = connect(
                user=self.username, password=self.password,
                host=self.hostname, database=self.name, port=self.port
            )

    def is_connected(self):
        return self.connection is not None

    def reset_cache(self):
        self.cache = {'table': None, 'sql': '', 'columns': [], 'values': []}

    def transaction_active(self):
        return self.cursor is not None

    def start_transaction(self):
        if not self.is_connected():
            print('There is no active connection to a database')
        elif self.transaction_active():
            print('There is already an active transaction')
        else:
            self.cursor = self.connection.cursor()

    def commit(self):
        if not self.is_connected():
            print('There is no active connection to a mysql database')
        elif not self.transaction_active():
            print('There is no active transaction')
        else:
            if len(self.cache['sql']) > 0:
                self.cursor.execute(self.cache['sql'], self.cache['values'])
                self.reset_cache()

            self.connection.commit()
            self.cursor.close()
            self.cursor = None
            self.reset_cache()

    def insert(self, table_name, columns, values):
        auto_transact = False
        assert len(columns) == len(values), 'columns length must match values length'
        if self.cursor is None:
            auto_transact = True
            self.start_transaction()

        if not auto_transact:
            if self.cache['table'] == table_name:
                if utils.array_equals(columns, self.cache['columns']) and len(self.cache['sql']) < SQL_MAX_LENGTH:
                    self.cache['values'] = self.cache['values'] + values
                    self.cache['sql'] += f', {generate_values_placeholders(len(values))}'
                else:
                    self.cursor.execute(self.cache['sql'], self.cache['values'])
                    self.reset_cache()
                    self.insert(table_name, columns, values)

            else:
                self.cache = {
                    'table': table_name,
                    'columns': columns,
                    'values': values,
                    'sql': 'INSERT INTO {} {} VALUES {}'.format(
                        table_name,
                        generate_columns_string(columns),
                        generate_values_placeholders(len(values))
                    )
                }

        elif self.cache['table'] is not None:
            self.cursor.execute(self.cache['sql'], self.cache['values'])
            self.reset_cache()
            self.insert(table_name, columns, values)

        else:
            insertion_statement = 'INSERT INTO {} {} VALUES {}'.format(
                table_name,
                generate_columns_string(columns),
                generate_values_placeholders(len(values))
            )
            self.cursor.execute(insertion_statement, values)

        if auto_transact:
            self.commit()

    def select(self, table_name, fields=None, where=None, limit=None):
        query = 'SELECT {} FROM {} {} {}'.format(
            '{}', table_name,
            '{}'.format(value_or_empty(where, 'where')),
            '{}'.format(value_or_empty(limit, 'limit'))
        )

        fields = [] if fields is None else fields
        if len(fields) == 0:
            query = query.format('*')
        else:
            query = query.format(utils.stringify(fields))

        if self.cursor is not None:
            print('Cannot select while a transaction is currently in progress')
            return None

        self.cursor = self.connection.cursor()
        self.cursor.execute(query)

        column_names = self.cursor.column_names
        results = self.cursor.fetchall()
        self.cursor = None

        json_results = []
        for result in results:
            json = {}
            for value_index in range(0, len(result)):
                json[column_names[value_index]] = result[value_index]

            json_results.append(json)

        return json_results

    def close(self):
        if self.is_connected():
            self.connection.close()
            self.connection = None

    def __del__(self):
        self.close()
