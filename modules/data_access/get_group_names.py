#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions


def get_group_names (connection: extensions.connection, group_prefix: str) -> List[str]:
    """
    Get group names for a group_prefix.

    :param connection: A database connection.
    :param group_id: The group prefix.
    :return: The group names.
    """
    sql = '''
        select
             g.group_name
        from 
             "group" g, group_member gm 
        where
        	g.group_id = gm.group_id 
        and
            g.group_name like %s
    '''

    groups: List[str] = []
    group_prefix_1: str = group_prefix+'%'
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [group_prefix_1])
        rows = cursor.fetchall()
        for row in rows:
            group_name = row[0]
            groups.append(group_name)
    return groups
