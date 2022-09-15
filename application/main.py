from common.modules import entity_map
from common.constants import entity_key

import sys

instantiated_entity_map = {}
fetched_data_set = set()


def execute(entities):

    for key in entities:
        if key not in entity_map:
            print(f'Could not find entity with key {key}')
            sys.exit(1)

        object_entity = entity_map[key]
        entity_dependencies = object_entity.dependencies()

        print(f'Executing entity {key}')
        if len(entity_dependencies):
            print(f'Entity {key} has dependencies')
            execute(entity_dependencies)

        instantiated_entity_map[key] = object_entity()
        instantiated_entity = instantiated_entity_map[key]

        for dependency_key in entity_dependencies:
            instantiated_entity.set_dependencies_cache(dependency_key, instantiated_entity_map[dependency_key])

        if key not in fetched_data_set:
            instantiated_entity.load_cache()
            instantiated_entity.fetch()
            instantiated_entity.save()

            fetched_data_set.add(key)


if __name__ == '__main__':
    root_entities = [
        entity_key.calendar_date,
        entity_key.calendar_holiday_date,
        entity_key.census_state_population,
        entity_key.census_county_population,
        entity_key.census_city_population,
        entity_key.cdc_state_cases,
        entity_key.cdc_state_deaths,
        entity_key.cdc_state_tests,
        entity_key.cdc_state_vaccinations
    ]
    execute(root_entities)
