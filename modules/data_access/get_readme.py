#!/usr/bin/env python3
from contextlib import closing

from data_access.db_connector import DbConnector


def get_readme(connector: DbConnector) -> bytes:
    """
    Get the README template for data product publication.

    :param connector: The database connector.
    :return: The template.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select b_val from {schema}.dp_kvp where key_name = 'readme' and version_end_date isnull
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        mem_view: memoryview = cursor.fetchone()[0]
        return mem_view.tobytes()
