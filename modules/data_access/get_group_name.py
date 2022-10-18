#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions


def get_named_location_group(connection: extensions.connection, grp_prefix: str) -> List[str]:
    """
    Get context entries for a named location.

    :param connection: A database connection.
    :param named_location_id: The named location ID.
    :return: The context entries.
    """
    sql = '''
        select
        g.group_name
        from named_location_group nlg, "group" g
        where
            nlg.group_id = g.group_id and g.group_name like '%s%%'"
    '''

    groups: List[str] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            group_name = row[0]
            groups.append(group_name)
    return groups
