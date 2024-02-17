from common.constants import entity_key
from common.service import cached_request
from datetime import date, datetime

import csv
import io


URL = 'https://raw.githubusercontent.com/washingtonpost/data-police-shootings/master/v2/fatal-police-shootings-data.csv'


def fetch_police_shooting_data():
    request = cached_request(entity_key.police_fatal_shootings, 'GET', URL)
    raw_data = csv.DictReader(io.StringIO(request.content.decode('utf-8')))

    start_of_2020 = date(2020, 1, 1)
    records = list(raw_data)
    return [record for record in records if start_of_2020 <= datetime.fromisoformat(record['date']).date()]
