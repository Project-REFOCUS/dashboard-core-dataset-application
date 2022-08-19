from calendar.entity import CalendarDate, CalendarMonth, CalendarHoliday, CalendarHolidayType, CalendarDay
from common.constants import entity_key

entity_map = {
    entity_key.calendar_date: CalendarDate,
    entity_key.calendar_day: CalendarDay,
    entity_key.calendar_month: CalendarMonth,
    entity_key.calendar_holiday: CalendarHoliday,
    entity_key.calendar_holiday_type: CalendarHolidayType
}
