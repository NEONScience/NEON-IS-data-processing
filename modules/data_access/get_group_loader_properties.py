#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector
from data_access.types.property import Property


def get_group_loader_properties(connector: DbConnector, group_id: int) -> List[Property]:
    """
    Get the properties associated with a group id.

    :param connector: A database connection.
    :param group_id: The group ID to search.
    :return: The group id properties.
    """
    sql = '''
        select
            g.group_name,
            g.hor,
            g.ver
        from
            "group" g
        where
            g.group_id = %s
    '''
    properties: List[Property] = []
    hor_name = "HOR"
    ver_name = "VER"
    with closing(connector.get_connection().cursor()) as cursor:
        cursor.execute(sql, [group_id])
        rows = cursor.fetchall()
        for row in rows:
            # name = row[0]
            hor = row[1]
            ver = row[2]
            properties.append(Property(name=hor_name, value=hor))
            properties.append(Property(name=ver_name, value=ver))
    return properties
