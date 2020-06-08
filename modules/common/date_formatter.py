#!/usr/bin/env python3
from datetime import datetime, timedelta
from pathlib import Path

ISO_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def convert(date: datetime) -> str:
    """
    Convert a datetime object to an ISO standard string.

    :param date: The datetime object to convert.
    :return: An ISO date string of the date.
    """
    return date.strftime(ISO_FORMAT)


def parse(date: str) -> datetime:
    """
    Convert an ISO standard string to a datetime object.

    :param date: A date and time string in ISO format.
    :return: A datetime object representing the date.
    """
    return datetime.strptime(date, ISO_FORMAT)


def parse_date_path(date_path: Path) -> datetime:
    """
    Parse the timestamp path to a datetime object.

    :param date_path: The path containing the timestamp file.
    :return: A datetime object representing the date.
    """
    date = str(*date_path.parts[3:])
    return datetime.strptime(date, ISO_FORMAT)


def parse_date(date: datetime.date):
    """
    Parse formatted year, month, and day strings from a date object.

    :param date: A date object.
    :return: Tuple of year, month, and day strings.
    """
    dt = datetime(date.year, date.month, date.day)
    year = dt.strftime('%Y')
    month = dt.strftime('%m')
    day = dt.strftime('%d')
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
        date = start_date + timedelta(days=i)
        yield date
