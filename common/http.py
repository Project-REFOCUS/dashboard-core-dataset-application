from common.logger import Logger

import requests
import time
import json

logger = Logger(__name__)


def send_request(method, url, retries, backoff, encoding='utf-8'):
    try:
        response = requests.request(method=method, url=url)
        if response.status_code == 204:
            return {'status': response.status_code, 'message': response.text}

        if response.status_code != 200:
            logger.debug(f'Received status code [{response.status_code}] for url {url}.')
            if retries > 0:
                time.sleep(backoff)
                logger.debug(f'Backing off for {backoff} seconds and will retry {retries} more time(s)')
            else:
                logger.debug(f'Retries have been exhausted')

            error_response = {'status': response.status_code, 'message': response.text}
            return send_request(method, url, retries - 1, backoff) if retries > 0 else error_response
        content = json.loads(response.content.decode(encoding))
        return content
    except requests.exceptions.ConnectionError:
        return send_request(method, url, retries - 1, backoff) if retries > 0 else None
    except json.JSONDecodeError:
        return None


def get(url, retries=5, backoff=3, encoding='utf-8'):
    return send_request('GET', url, retries=retries, backoff=backoff, encoding=encoding)
