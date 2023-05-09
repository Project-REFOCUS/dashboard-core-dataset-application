from common.modules import entity_map
from common.constants import entity_key
from common.utils import debug

import sys

instantiated_entity_map = {}
fetched_data_set = set()


def execute(entities):

    for key in entities:
        if key not in entity_map:
            print(f'Could not find entity with key {key}')
            sys.exit(1)

        debug(f'Started {key}...')
        object_entity = entity_map[key]
        entity_dependencies = object_entity.dependencies()

        if len(entity_dependencies):
            debug(f'Entity {key} has dependencies ...{entity_dependencies}')
            execute(entity_dependencies)
            debug(f'Finished executing dependencies for {key}...')

        instantiated_entity_map[key] = object_entity()
        instantiated_entity = instantiated_entity_map[key]

        for dependency_key in entity_dependencies:
            instantiated_entity.set_dependencies(dependency_key, instantiated_entity_map[dependency_key])

        if key not in fetched_data_set:
            debug(f'Loading {key} cache...')
            instantiated_entity.load_cache()
            instantiated_entity.fetch()
            while instantiated_entity.has_data():
                debug(f'Entity {key} has data and saving...')
                instantiated_entity.save()
                instantiated_entity.after_save()

                if instantiated_entity.has_updates():
                    debug(f'Entity {key} has updates and updating...')
                    instantiated_entity.update()
                    instantiated_entity.after_update()

            fetched_data_set.add(key)
        else:
            debug(f'Entity {key} already fetched...reloading cache...')
            instantiated_entity.load_cache()


if __name__ == '__main__':
    root_entities = [
        entity_key.census_block_group,
        entity_key.calendar_date,
        entity_key.calendar_holiday_date,
        entity_key.census_state_population,
        entity_key.census_county_population,
        entity_key.census_city_population,
        entity_key.cdc_state_cases,
        entity_key.cdc_state_deaths,
        entity_key.cdc_state_tests,
        entity_key.cdc_state_vaccinations,
        entity_key.police_fatal_shootings,
        entity_key.apha_racism_declarations,
        entity_key.ucla_covid_behind_bars,
        entity_key.osha_closed_complaints,
        entity_key.twitter_account,
        entity_key.twitter_tweets,
        entity_key.twitter_tweets_terms,
        entity_key.twitter_tweets_terms_frequency,
        entity_key.cdc_waste_water,
        entity_key.epa_ejscreen_value
    ]
    debug('Application starting...')
    execute(root_entities)
    debug('Application finished...')
