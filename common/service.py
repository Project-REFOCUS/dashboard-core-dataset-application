import requests

cache = {}


def cached_request(cache_id, method, url):
    if cache_id not in cache:
        cache[cache_id] = requests.request(method, url)

    return cache[cache_id]


def remove_cached_request(cache_id):
    if cache_id in cache:
        del cache[cache_id]
