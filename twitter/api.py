from requests.auth import HTTPBasicAuth

import requests
import json
import os

BASE_URL = 'https://api.twitter.com'


class TwitterApi:

    def __init__(self):
        self.api_client = os.getenv('TWITTER_API_CLIENT')
        self.api_secret = os.getenv('TWITTER_API_SECRET')
        self.api_token = None

    def authenticate(self):
        url = '/oauth2/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {'grant_type': 'client_credentials'}
        auth = HTTPBasicAuth(self.api_client, self.api_secret)
        response = requests.request('POST', f'{BASE_URL}{url}', headers=headers, data=body, auth=auth)

        if response.status_code == 200:
            self.api_token = json.loads(response.content.decode('utf-8'))['access_token']

    def is_authenticated(self):
        return self.api_token is not None

    def get_users_by_usernames(self, usernames):
        usernames_param = ','.join(usernames)
        headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {self.api_token}'}
        response = requests.request('GET', f'{BASE_URL}/2/users/by?usernames={usernames_param}', headers=headers)
        return json.loads(response.content.decode('utf-8'))['data'] if response.status_code == 200 else []
