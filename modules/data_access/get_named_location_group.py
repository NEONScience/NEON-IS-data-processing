#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions


def get_named_location_group(connection: extensions.connection, named_location_id: int) -> List[int]:
    """
    Get context entries for a named location.

    :param connection: A database connection.
    :param named_location_id: The named location ID.
    :return: The context entries.
    """
    sql = '''
        select 
            group_id
        from 
            named_location_group
        where 
            named_location_id = %s
    '''
    groups: List[int] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            group_id = row[0]
            groups.append(group_id)
    return groups
