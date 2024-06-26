from common.modules import entity_map
from common.constants import entity_key
from common.logger import Logger
from common.performance import PerformanceLogger
from newrelic import agent as newrelic_agent
from newrelic.api import application as newrelic_application

import sys

performance_logger = PerformanceLogger('application.main')
logger = Logger('application.main')

instantiated_entity_map = {}
fetched_data_set = set()


def execute(entities):
    for key in entities:
        if key not in entity_map:
            print(f'Could not find entity with key {key}')
            sys.exit(1)

        logger.debug(f'Started {key}...')
        object_entity = entity_map[key]
        entity_dependencies = object_entity.dependencies()

        if len(entity_dependencies):
            logger.debug(f'Entity {key} has dependencies ...{entity_dependencies}')
            execute(entity_dependencies)
            logger.debug(f'Finished executing dependencies for {key}...')

        instantiated_entity_map[key] = object_entity()
        instantiated_entity = instantiated_entity_map[key]

        for dependency_key in entity_dependencies:
            instantiated_entity.set_dependencies(dependency_key, instantiated_entity_map[dependency_key])

        if key not in fetched_data_set:
            module_timer = performance_logger.start(f'Executing module {key}')
            logger.debug(f'Loading {key} cache...')
            instantiated_entity.load_cache()
            if instantiated_entity.should_fetch_data():
                logger.debug(f'Fetching data for {key}...')
                instantiated_entity.fetch()
            else:
                logger.debug(f'Skipping fetch for {key}...')

            while instantiated_entity.has_data():
                logger.debug(f'Entity {key} has data and saving...')
                instantiated_entity.save()
                instantiated_entity.after_save()

                if instantiated_entity.has_updates():
                    logger.debug(f'Entity {key} has updates and updating...')
                    instantiated_entity.update()
                    instantiated_entity.after_update()

            fetched_data_set.add(key)
            module_timer.stop()
        else:
            logger.debug(f'Entity {key} already fetched...reloading cache...')
            instantiated_entity.load_cache()


if __name__ == '__main__':
    newrelic_agent.initialize()
    newrelic_application.register_application()
    root_entities = [
        entity_key.census_us_metro_area,
        entity_key.census_us_city_zipcode,
        entity_key.snap_nyc_data,
        entity_key.mvc_nyc_crashes,
        entity_key.census_block_group,
        entity_key.calendar_date,
        entity_key.calendar_holiday_date,
        entity_key.census_state_population,
        entity_key.census_county_population,
        entity_key.census_city_population,
        entity_key.census_employment_status,
        entity_key.cdc_state_cases,
        entity_key.cdc_state_deaths,
        entity_key.cdc_state_tests,
        entity_key.cdc_state_vaccinations,
        entity_key.cdc_county_deaths,
        entity_key.police_race_ethnicity,
        entity_key.apha_racism_declarations,
        entity_key.ucla_covid_behind_bars,
        entity_key.osha_closed_complaints,
        # entity_key.twitter_account,
        # entity_key.twitter_tweets,
        # entity_key.twitter_tweets_terms,
        # entity_key.twitter_tweets_terms_frequency,
        entity_key.cdc_waste_water,
        entity_key.epa_ejscreen_value,
        entity_key.childcare_center,
        entity_key.wholesale_market,
        entity_key.census_zipcode_population,
        entity_key.census_tract_population,
        entity_key.census_block_group_population,
    ]
    logger.debug('Application starting...')
    execute(root_entities)
    logger.debug('Application finished...')
