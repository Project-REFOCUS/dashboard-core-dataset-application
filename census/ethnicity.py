from entity.abstract import ResourceEntity


# TODO: Create an abstract CachedResourceEntity class to share logic of resources that don't require fetches or saves
class RaceEthnicity(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'race_ethnicity'
        self.record_cache = None
        self.records = []
        self.fields = []

    def load_cache(self):
        cachable_fields = ['name']
        records = self.mysql_client.select(self.table_name)
        for record in records:
            if self.record_cache is None:
                self.record_cache = {}

            for field in cachable_fields:
                self.record_cache[record[field]] = record

    def save(self):
        pass
