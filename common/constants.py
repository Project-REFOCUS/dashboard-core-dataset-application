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
    census_state_population = 'census.state_population'
    census_county_population = 'census.county_population'
    census_city_population = 'census.city_population'
    census_race_ethnicity = 'census.race_ethnicity'
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


class CacheId:
    list_of_states = 'list_of_states'
    us_trend_by = 'us_trend_by'
    twitter_accounts = 'twitter_accounts'


entity_key = EntityKey()
cache_id = CacheId()
