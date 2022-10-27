#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector


def get_named_location_context(connector: DbConnector, named_location_id: int) -> List[str]:
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
            context_code
        from 
            {schema}.named_location_context 
        where 
            named_location_id = %s
    '''
    contexts: List[str] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            context_code = row[0]
            contexts.append(context_code)
    return contexts
