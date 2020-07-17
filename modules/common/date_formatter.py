#!/usr/bin/env python3
from datetime import datetime


def get_format() -> str:
    return '%Y-%m-%dT%H:%M:%SZ'


def to_string(dt: datetime) -> str:
    """
    Convert a datetime object to formatted string.

    :param dt: The datetime object.
    :return: A formatted date.
    """
    return dt.strftime(get_format())


def to_datetime(dt: str) -> datetime:
    """
    Convert a formatted date string to a datetime object.

    :param dt: A formatted date and time string.
    :return: A datetime object representing the date.
    """
    return datetime.strptime(dt, get_format())
