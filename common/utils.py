from datetime import datetime
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
