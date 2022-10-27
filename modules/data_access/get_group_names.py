#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions


def get_group_names (connection: extensions.connection, group_id: int) -> List[str]:
    """
    Get group names for a group_prefix.

    :param connection: A database connection.
    :param mem_group_id: The member group id.
    :return: The group names.
    """
    sql = '''
        select  
             g.group_name
        from 
             "group" g 
        where
        	g.group_id = %s 
    '''

    group_names: List[str] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [group_id])
        rows = cursor.fetchall()
        for row in rows:
            group_names = row[0]
    return group_names
