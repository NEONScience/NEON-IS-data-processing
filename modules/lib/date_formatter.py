#!/usr/bin/env python3
from datetime import datetime


def convert(date_time):
    """
    Convert a datetime object to an ISO standard string.

    :param date_time: The date object.
    :type date_time: datetime
    """
    if date_time is not None:
        return date_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        return date_time


def parse(date_time):
    """
    Convert an ISO standard string to a datetime object.

    :param date_time A date and time.
    :type date_time: str
    """
    if date_time is not None:
        return datetime.strptime(date_time, '%Y-%m-%dT%H:%M:%SZ')
    else:
        return date_time
