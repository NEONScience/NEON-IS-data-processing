#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector


def get_named_location_group(connector: DbConnector, named_location_id: int) -> List[str]:
    """
    Get context entries for a named location.

    :param connector: A database connection.
    :param named_location_id: The named location ID.
    :return: The context entries.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            g.group_name
        from 
            {schema}.named_location_group nlg, {schema}."group" g
        where
            nlg.group_id = g.group_id and nlg.named_location_id = %s
    '''
    groups: List[str] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            group_name = row[0]
            groups.append(group_name)
    return groups
