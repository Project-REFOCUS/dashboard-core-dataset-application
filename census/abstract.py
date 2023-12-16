from entity.abstract import ResourceEntity

import requests
import json


class CensusPopulationResourceEntity(ResourceEntity):
    
    def fetch_resource(self, api_path, name):
        api_url = 'https://data.census.gov/api/access/data/table' + api_path
        
        self.records = []
        duplicate_set = set()
            
        response = json.loads(requests.request('GET', api_url).content.decode('utf-8'))
        data = response['response']['data']
        # Note: the first element is improper
        data.pop(0)
        population_index = 2
        identity_index = 5
        for record in data:
            if record[identity_index] not in duplicate_set:
                duplicate_set.add(record[identity_index])
                self.records.append({'population': record[population_index], name: record[identity_index]})
