from entity.abstract import ResourceEntity


# TODO: Create an abstract CachedResourceEntity class to share logic of resources that don't require fetches or saves
class RaceEthnicity(ResourceEntity):

    def __init__(self):
        super().__init__()

        self.table_name = 'race_ethnicity'
        self.record_cache = None
        self.records = []
        self.fields = []
        self.cacheable_fields = ['name']

    def save(self):
        pass
