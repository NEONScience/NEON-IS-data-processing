#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions

from data_access.types.active_period import ActivePeriod


def get_active_periods(connection: extensions.connection, group_id: int) -> List[ActivePeriod]:
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
            group_active_period 
        where 
            group_id = %s
    '''
    periods: List[ActivePeriod] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [group_id])
        rows = cursor.fetchall()
        for row in rows:
            start_date = row[0]
            end_date = row[1]
            periods.append(ActivePeriod(start_date=start_date, end_date=end_date))
    return periods
