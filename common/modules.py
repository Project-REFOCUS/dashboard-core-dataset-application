from datecalendar.entity import CalendarDate, CalendarMonth, CalendarDay
from datecalendar.holiday import CalendarHolidayDate, CalendarHoliday, CalendarHolidayType
from census.state import USState
from common.constants import entity_key

entity_map = {
    entity_key.calendar_date: CalendarDate,
    entity_key.calendar_day: CalendarDay,
    entity_key.calendar_month: CalendarMonth,
    entity_key.calendar_holiday: CalendarHoliday,
    entity_key.calendar_holiday_date: CalendarHolidayDate,
    entity_key.calendar_holiday_type: CalendarHolidayType,
    entity_key.census_us_state: USState
}
