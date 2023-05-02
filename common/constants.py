class EntityKey:
    calendar_date = 'calendar.date'
    calendar_day = 'calendar.day'
    calendar_holiday = 'calendar.holiday'
    calendar_holiday_date = 'calendar.holiday_date'
    calendar_holiday_type = 'calendar.holiday_type'
    calendar_month = 'calendar.month'
    census_us_county = 'census.us_county'
    census_us_state = 'census.us_state'
    census_us_city = 'census.us_city'
    census_us_city_type = 'census.us_city_type'
    census_us_county_city = 'census.us_county_city'
    census_state_population = 'census.state_population'
    census_county_population = 'census.county_population'
    census_city_population = 'census.city_population'
    census_race_ethnicity = 'census.race_ethnicity'
    census_tract = 'census.tract'
    census_block_group = 'census.block_group'
    cdc_state_cases = 'cdc.state_cases'
    cdc_state_deaths = 'cdc.state_deaths'
    cdc_state_tests = 'cdc.state_tests'
    cdc_state_vaccinations = 'cdc.state_vaccinations'
    police_fatal_shootings = 'police.fatal_shootings'
    apha_racism_declarations = 'apha.racism_declarations'
    ucla_covid_behind_bars = 'ucla.covid_behind_bars'
    osha_closed_complaints = 'osha.osha_closed_complaints'
    twitter_account = 'twitter.accounts'
    twitter_account_type = 'twitter.account_type'
    twitter_tweets = 'twitter.tweets'
    twitter_tweets_terms = 'twitter.tweets_terms'
    twitter_tweets_terms_frequency = 'twitter.tweets_terms_frequency'
    cdc_waste_water = 'cdc.waste_water'


class CacheId:
    list_of_states = 'list_of_states'
    us_trend_by = 'us_trend_by'
    twitter_accounts = 'twitter_accounts'
    tweets_by_twitter_id = 'tweets_by_twitter_id'
    tweets_by_text_and_id = 'tweets_by_text_and_id'


class Constants:
    twitter_api_search_start_time = 'TWITTER_API_SEARCH_START_TIME'
    twitter_api_recent_search = '/2/tweets/search/recent'
    twitter_api_full_search = '/2/tweets/search/all'
    twitter_api_tweet_count = '/2/tweets/counts/all'
    datetime_format = '%Y-%m-%dT%H:%M:%S.000Z'
    date_regex = r'^20[1-3][0-9]-[01][1-9]-[0-3][0-9]T\d{2}:\d{2}:\d{2}.\d{3}Z$'


entity_key = EntityKey()
cache_id = CacheId()
constants = Constants()
