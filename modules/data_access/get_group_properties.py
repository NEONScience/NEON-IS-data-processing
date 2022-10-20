#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions

import common.date_formatter as date_formatter
from data_access.types.property import Property


def get_group_properties(connection: extensions.connection, group_id: int) -> List[Property]:
    """
    Get the properties associated with a named location.

    :param connection: A database connection.
    :param named_location_id: The named location ID to search.
    :return: The named location properties.
    """
    sql = '''
        select
            group.name,
            group.hor,
            group.ver
        from
            group
        where
            group_id = %s
    '''
    properties: List[Property] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [group_id])
        rows = cursor.fetchall()
        for row in rows:
            name = row[0]
            hor = row[1]
            ver = row[2]
            properties.append(Property(name=name, value=hor))
            properties.append(Property(name=name, value=ver))
    return properties
