#!/usr/bin/env python3
from datetime import datetime, timedelta, date
from pathlib import Path

ISO_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def convert(date_time: datetime) -> str:
    """
    Convert a datetime object to an ISO standard string.

    :param date_time: The datetime object to convert.
    :return: An ISO date string of the date.
    """
    return date_time.strftime(ISO_FORMAT)


def parse(date_string: str) -> datetime:
    """
    Convert an ISO standard string to a datetime object.

    :param date_string: A date and time string in ISO format.
    :return: A datetime object representing the date.
    """
    return datetime.strptime(date_string, ISO_FORMAT)


def parse_date_path(date_path: Path) -> datetime:
    """
    Parse the timestamp path to a datetime object.

    :param date_path: The path containing the timestamp file.
    :return: A datetime object representing the date.
    """
    date_parts = str(*date_path.parts[3:])
    return datetime.strptime(date_parts, ISO_FORMAT)


def parse_date(date_obj: datetime.date):
    """
    Parse formatted year, month, and day strings from a date object.

    :param date_obj: A date object.
    :return: Tuple of year, month, and day strings.
    """
    date_time = datetime(date_obj.year, date_obj.month, date_obj.day)
    year = date_time.strftime('%Y')
    month = date_time.strftime('%m')
    day = date_time.strftime('%d')
    return year, month, day


def dates_between(start_date: datetime, end_date: datetime):
    """
    Yield all dates between the start and end dates.

    :param start_date: Date to begin generating dates.
    :param end_date: Date to stop generating dates.
    :return: All dates between the start and end dates.
    """
    delta = end_date - start_date
    for i in range(delta.days + 1):
        yield start_date + timedelta(days=i)


def date_is_between(year: int, month: int, day: int, start_date: date, end_date: date):
    """
    Is the date represented by year, month, and day between the start and end dates.

    :param year: The date year.
    :param month: The date month.
    :param day: The date day.
    :param start_date: A start date.
    :param end_date: An end date.
    :return: True if between, False otherwise.
    """
    if start_date is None and end_date is None:
        return True
    if start_date is not None and end_date is not None:
        d = date(year, month, day)
        if start_date < d < end_date:
            return True
    return False
