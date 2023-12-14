import requests
import time
import json


def send_request(method, url, retries, backoff, encoding='utf-8'):
    try:
        response = requests.request(method=method, url=url)
        if response.status_code != 200:
            time.sleep(backoff)
            error_response = {'status': response.status_code, 'message': response.text}
            return send_request(method, url, retries - 1, backoff) if retries > 0 else error_response
        content = json.loads(response.content.decode(encoding))
        return content
    except requests.exceptions.ConnectionError:
        return send_request(method, url, retries - 1, backoff) if retries > 0 else None


def get(url, retries=5, backoff=3):
    return send_request('GET', url, retries=retries, backoff=backoff)
