from datetime import date


def date_between(year, month, day, start_date, end_date):
    """
    Is the date represented by year, month, and day between the start and end dates.

    :param year: The date year.
    :type year: int
    :param month: The date month.
    :type month: int
    :param day: The date day.
    :type day: int
    :param start_date: The start date.
    :type start_date: date
    :param end_date: The end date.
    :type end_date: date
    :return: True if between, False otherwise.
    """
    if start_date is None and end_date is None:
        return True
    if start_date is not None and end_date is not None:
        d = date(year, month, day)
        if start_date < d < end_date:
            return True
    return False
