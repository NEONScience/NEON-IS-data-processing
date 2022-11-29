#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector


def get_threshold_context(connector: DbConnector, threshold_uuid: str) -> List[str]:
    """
    Get all context codes for a threshold.

    :param connector: A database connection.
    :param threshold_uuid: The threshold UUID.
    :return: The context codes.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    context_codes: List[str] = []
    with closing(connection.cursor()) as cursor:
        sql = f'''
            select 
                context_code 
            from 
                {schema}.threshold_context 
            where
                threshold_uuid = %s
        '''
        cursor.execute(sql, [threshold_uuid])
        rows = cursor.fetchall()
        for row in rows:
            context_code = row[0]
            context_codes.append(context_code)
    return context_codes
