#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions


def get_named_location_context(connection: extensions.connection, named_location_id: int) -> List[str]:
    """
    Get context entries for a named location.

    :param connection: A database connection.
    :param named_location_id: The named location ID.
    :return: The context entries.
    """
    sql = '''
        select 
            context_code
        from 
            named_location_context 
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
