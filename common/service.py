from database.mysqldb import MysqlClient
import requests

cache = {}


def cached_request(cache_id, method, url):
    if cache_id not in cache:
        cache[cache_id] = requests.request(method, url)

    return cache[cache_id]


def remove_cached_request(cache_id):
    if cache_id in cache:
        del cache[cache_id]


def cached_query(cache_id, table_name, fields=None, where=None, limit=None):
    cache_key = f'query__{cache_id}'
    if cache_key not in cache:
        mysql_client = MysqlClient()
        mysql_client.connect()
        if mysql_client.is_connected():
            cache[cache_key] = mysql_client.select(table_name, fields, where, limit)
        else:
            print('Could not connect to the database')

    return cache[cache_key]
