from datecalendar.entity import CalendarDate, CalendarMonth, CalendarDay
from datecalendar.holiday import CalendarHolidayDate, CalendarHoliday, CalendarHolidayType
from census.city import USCity, USCityType, USCountyCities, CityPopulation
from census.county import USCounty, CountyPopulation
from census.state import USState, StatePopulation
from census.metro import USMetroArea
from census.zipcode import USZipCode, USCityZipCodes, ZipcodePopulation
from census.ethnicity import RaceEthnicity
from census.tract import CensusTract, TractPopulation
from census.blockgroup import BlockGroup, BlockGroupPopulation
from census.employment import EmploymentStatus
from common.constants import entity_key
from cdc.cases import StateCases
from cdc.deaths import StateDeaths
from cdc.tests import StateTests
from cdc.deaths import NycCountyDeaths
from cdc.vaccinations import StateVaccinations
from epa.ejscreen import EpaDataField, EpaDataValue
from police.entity import FatalShootings
from apha.racism import RacismDeclarations
from ucla.covid import CovidBehindBars
from osha.complaints import OshaClosedComplaints
from mvc.crashes import NYCMotorVehicleCollisions
from snap.nyc import NYCCountySnap
from twitter.accounts import TwitterAccountType, TwitterAccount
from twitter.tweets import Tweets
from twitter.terms import TwitterTerms, TwitterTermsFrequency
from cdc.wastewater import WasteWater
from childcare.entity import ChildCareCenter
from childcare.caretype import ChildCareType
from wholesale.entity import WholesaleMarket
from wholesale.application import MarketApplicationType
from census.tract import CensusTractAccentUpdate
from census.blockgroup import BlockGroupAccentUpdate


entity_map = {
    entity_key.calendar_date: CalendarDate,
    entity_key.calendar_day: CalendarDay,
    entity_key.calendar_month: CalendarMonth,
    entity_key.calendar_holiday: CalendarHoliday,
    entity_key.calendar_holiday_date: CalendarHolidayDate,
    entity_key.calendar_holiday_type: CalendarHolidayType,
    entity_key.census_us_city: USCity,
    entity_key.census_us_city_type: USCityType,
    entity_key.census_us_county_city: USCountyCities,
    entity_key.census_us_county: USCounty,
    entity_key.census_us_state: USState,
    entity_key.census_us_metro_area: USMetroArea,
    entity_key.census_us_zipcode: USZipCode,
    entity_key.census_us_city_zipcode: USCityZipCodes,
    entity_key.census_state_population: StatePopulation,
    entity_key.census_county_population: CountyPopulation,
    entity_key.census_city_population: CityPopulation,
    entity_key.census_race_ethnicity: RaceEthnicity,
    entity_key.census_tract: CensusTract,
    entity_key.census_block_group: BlockGroup,
    entity_key.census_employment_status: EmploymentStatus,
    entity_key.cdc_state_cases: StateCases,
    entity_key.cdc_state_deaths: StateDeaths,
    entity_key.cdc_state_tests: StateTests,
    entity_key.cdc_county_deaths: NycCountyDeaths,
    entity_key.cdc_state_vaccinations: StateVaccinations,
    entity_key.epa_ejscreen_field: EpaDataField,
    entity_key.epa_ejscreen_value: EpaDataValue,
    entity_key.police_fatal_shootings: FatalShootings,
    entity_key.apha_racism_declarations: RacismDeclarations,
    entity_key.ucla_covid_behind_bars: CovidBehindBars,
    entity_key.osha_closed_complaints: OshaClosedComplaints,
    entity_key.twitter_account_type: TwitterAccountType,
    entity_key.twitter_account: TwitterAccount,
    entity_key.twitter_tweets: Tweets,
    entity_key.twitter_tweets_terms: TwitterTerms,
    entity_key.twitter_tweets_terms_frequency: TwitterTermsFrequency,
    entity_key.cdc_waste_water: WasteWater,
    entity_key.snap_nyc_data: NYCCountySnap,
    entity_key.mvc_nyc_crashes: NYCMotorVehicleCollisions,
    entity_key.childcare_center: ChildCareCenter,
    entity_key.childcare_center_type: ChildCareType,
    entity_key.wholesale_market_app_type: MarketApplicationType,
    entity_key.wholesale_market: WholesaleMarket,
    entity_key.census_zipcode_population: ZipcodePopulation,
    entity_key.census_tract_population: TractPopulation,
    entity_key.census_block_group_population: BlockGroupPopulation,
    entity_key.census_tract_accent_update: CensusTractAccentUpdate,
    entity_key.census_block_group_accent_update: BlockGroupAccentUpdate
}
