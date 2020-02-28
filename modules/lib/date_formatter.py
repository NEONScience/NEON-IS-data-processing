from datetime import datetime


def convert(date_time):
    """
    Return a datetime as an ISO standard string.
    :param date_time A datetime object.
    """
    if date_time is not None:
        return date_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        return date_time


def parse(date_time):
    """
    Return a datetime from an ISO standard string.
    :param date_time A string date time.
    """
    if date_time is not None:
        return datetime.strptime(date_time, '%Y-%m-%dT%H:%M:%SZ')
    else:
        return date_time
