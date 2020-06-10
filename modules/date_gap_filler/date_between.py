#!/usr/bin/env python3
from datetime import date


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
