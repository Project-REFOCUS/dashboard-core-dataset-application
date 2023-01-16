from datetime import datetime, timedelta

import math
import sys
import os


def array_equals(array_one, array_two):
    length = len(array_one)
    equals = length == len(array_two)
    index = 0
    while index < length and equals:
        equals = array_one[index] == array_two[index]
        index += 1

    return equals


def stringify(array):
    string = ''
    delimiter = ''
    for a in array:
        string += delimiter
        string += a
        delimiter = ','

    return string


def from_string_to_date(date_string, format_string):
    return datetime.strptime(date_string, format_string).date()


def int_or_none(value):
    try:
        value = int(value)
    except ValueError:
        value = None
    except TypeError:
        value = None

    return value


def ensure_int(value):
    try:
        value = int(value)
    except ValueError:
        value = 0
    except TypeError:
        value = 0
    return value


def ensure_float(value):
    try:
        value = float(value)
    except ValueError:
        value = 0.0
    except TypeError:
        value = 0.0
    return value


def log(message, newline=True, show_timestamp=True):
    timestamp = str(datetime.now()) if show_timestamp else ''
    sys.stdout.write('\033[1m{}\033[0m {}{}'.format(timestamp, message, '\n' if newline else ''))


def percentage(processed, amount):
    quotient = processed / amount
    return f'{math.floor(quotient * 100)}%'


def progress(value, total):
    if os.getenv('DEBUG_PROGRESS') is not None:
        newline = value == total
        log(f'\rProgress: {percentage(value, total)} - Records processed: {value} of {total}', newline, False)


def debug(message):
    if os.getenv('DEBUG_PROGRESS') is not None:
        log(message)


def start_of_day(daytime):
    return daytime - timedelta(hours=daytime.hour, minutes=daytime.minute, seconds=daytime.second, microseconds=daytime.microsecond)
