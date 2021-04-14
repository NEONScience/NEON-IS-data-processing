#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions


def get_threshold_context(connection: extensions.connection, threshold_uuid: str) -> List[str]:
    """
    Get all context codes for a threshold.

    :param connection: A database connection.
    :param threshold_uuid: The threshold UUID.
    :return: The context codes.
    """
    context_codes: List[str] = []
    with closing(connection.cursor()) as cursor:
        sql = '''
            select 
                context_code 
            from 
                threshold_context 
            where 
                threshold_uuid = %s
        '''
        cursor.execute(sql, [threshold_uuid])
        rows = cursor.fetchall()
        for row in rows:
            context_code = row[0]
            context_codes.append(context_code)
    return context_codes
