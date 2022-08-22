from datecalendar.entity import CalendarDate, CalendarMonth, CalendarDay
from datecalendar.holiday import CalendarHolidayDate, CalendarHoliday, CalendarHolidayType
from census.city import USCity, CityPopulation
from census.county import USCounty, CountyPopulation
from census.state import USState, StatePopulation
from common.constants import entity_key

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
    entity_key.census_city_population: CityPopulation
}
