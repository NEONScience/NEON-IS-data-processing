#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector
from data_access.types.active_period import ActivePeriod


def get_group_loader_active_periods(connector: DbConnector, group_id: int) -> List[ActivePeriod]:
    """
    Get the active time periods for a group id.

    :param connector: A database connector.
    :param group_id: A group ID.
    :return: The active periods.
    """
    sql = '''
        select 
            start_date, end_date 
        from 
            group_active_period 
        where 
            group_id = %s
        order by start_date
    '''
    periods: List[ActivePeriod] = []
    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [group_id])
        rows = cursor.fetchall()
        for row in rows:
            start_date = row[0]
            end_date = row[1]
            periods.append(ActivePeriod(start_date=start_date, end_date=end_date))
    return periods
