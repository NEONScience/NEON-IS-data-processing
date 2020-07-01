#!/usr/bin/env python3
from contextlib import closing
from typing import List

from cx_Oracle import Connection


def get_threshold_context(connection: Connection, condition_uuid: str) -> List[str]:
    """
    Get all context entries for a threshold.

    :param connection: A database connection.
    :param condition_uuid: The condition UUID.
    :return: The context codes.
    """
    context_codes: List[str] = []
    with closing(connection.cursor()) as cursor:
        sql = '''
            select 
                context_code 
            from 
                condition_context 
            where 
                condition_uuid = :condition_uuid
        '''
        rows = cursor.execute(sql, condition_uuid=condition_uuid)
        for row in rows:
            context_codes.append(row[0])
    return context_codes
