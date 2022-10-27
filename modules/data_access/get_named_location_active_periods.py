#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.types.active_period import ActivePeriod
from data_access.db_connector import DbConnector


def get_active_periods(connector: DbConnector, named_location_id: int) -> List[ActivePeriod]:
    """
    Get the active time periods for a named location.

    :param connector: A database connection.
    :param named_location_id: A named location ID.
    :return: The active periods.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select 
            start_date, end_date 
        from 
            {schema}.active_period 
        where 
            named_location_id = %s
    '''
    periods: List[ActivePeriod] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            start_date = row[0]
            end_date = row[1]
            periods.append(ActivePeriod(start_date=start_date, end_date=end_date))
    return periods
