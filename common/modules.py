from datecalendar.entity import CalendarDate, CalendarMonth, CalendarDay
from datecalendar.holiday import CalendarHolidayDate, CalendarHoliday, CalendarHolidayType
from census.city import USCity, CityPopulation
from census.county import USCounty, CountyPopulation
from census.state import USState, StatePopulation
from census.ethnicity import RaceEthnicity
from common.constants import entity_key
from cdc.cases import StateCases
from cdc.deaths import StateDeaths
from cdc.tests import StateTests
from cdc.vaccinations import StateVaccinations
from police.entity import FatalShootings
from apha.racism import RacismDeclarations
from ucla.covid import CovidBehindBars
from osha.complaints import OshaClosedComplaints

entity_map = {
    entity_key.calendar_date: CalendarDate,
    entity_key.calendar_day: CalendarDay,
    entity_key.calendar_month: CalendarMonth,
    entity_key.calendar_holiday: CalendarHoliday,
    entity_key.calendar_holiday_date: CalendarHolidayDate,
    entity_key.calendar_holiday_type: CalendarHolidayType,
    entity_key.census_us_city: USCity,
    entity_key.census_us_county: USCounty,
    entity_key.census_us_state: USState,
    entity_key.census_state_population: StatePopulation,
    entity_key.census_county_population: CountyPopulation,
    entity_key.census_city_population: CityPopulation,
    entity_key.census_race_ethnicity: RaceEthnicity,
    entity_key.cdc_state_cases: StateCases,
    entity_key.cdc_state_deaths: StateDeaths,
    entity_key.cdc_state_tests: StateTests,
    entity_key.cdc_state_vaccinations: StateVaccinations,
    entity_key.police_fatal_shootings: FatalShootings,
    entity_key.apha_racism_declarations: RacismDeclarations,
    entity_key.ucla_covid_behind_bars: CovidBehindBars,
    entity_key.osha_closed_complaints: OshaClosedComplaints
}
