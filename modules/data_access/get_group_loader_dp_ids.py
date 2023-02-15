#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector


def get_group_loader_dp_ids(connector: DbConnector, group_id: int) -> List[str]:
    """
    Get the active time periods for a group id.

    :param connector: A database connector.
    :param group_id: A group ID.
    :return: The data product ids.
    """
    sql = '''
        select 
            substring (dpg.dp_idq  from 15 for 13 )
        from 
            "group" g, data_product_group dpg
        where 
            dpg.group_id = g.group_id
        and
            g.group_id = %s
    '''
    dpids: List[str] = []
    connection = connector.get_connection()
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [group_id])
        rows = cursor.fetchall()
        for row in rows:
            data_product_id = row[0]
            dpids.append(data_product_id)
    return dpids
