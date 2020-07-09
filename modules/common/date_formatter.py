#!/usr/bin/env python3
from datetime import datetime


def get_format() -> str:
    return '%Y-%m-%dT%H:%M:%SZ'


def convert(dt: datetime) -> str:
    """
    Convert a datetime object to an ISO standard string.

    :param dt: The datetime object to convert.
    :return: An ISO date string of the date.
    """
    return dt.strftime(get_format())


def parse(dt: str) -> datetime:
    """
    Convert an ISO standard string to a datetime object.

    :param dt: A date and time string in ISO format.
    :return: A datetime object representing the date.
    """
    return datetime.strptime(dt, get_format())
