#!/usr/bin/env python3
from contextlib import closing
from typing import List

from cx_Oracle import Connection

from data_access.types.active_period import ActivePeriod


def get_active_periods(connection: Connection, named_location_id: int) -> List[ActivePeriod]:
    """
    Get the active time periods for a named location.

    :param connection: A database connection.
    :param named_location_id: A named location ID.
    :return: The active periods.
    """
    sql = '''
        select 
            start_date, end_date 
        from 
            active_period 
        where 
            named_location_id = :id
    '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, id=named_location_id)
        periods: List[ActivePeriod] = []
        for row in rows:
            start_date = row[0]
            if start_date is not None:
                end_date = row[1]
                periods.append(ActivePeriod(start_date=start_date, end_date=end_date))
    return periods
