"""
Module to read data product keywords from the database.
"""
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector


def get_keywords(connector: DbConnector, dp_idq: str) -> List[str]:
    """
    Get the data product keywords for the given IDQ.

    :param connector: A database connection.
    :param dp_idq: The data product idq.
    :return: The data product keywords.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select 
            keyword.word 
        from 
            {schema}.keyword, {schema}.dp_keyword
        where
            dp_keyword.dp_idq = %s
        and 
            dp_keyword.keyword_id = keyword.keyword_id
    '''
    keywords = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [dp_idq])
        rows = cursor.fetchall()
        for row in rows:
            keyword = row[0]
            keywords.append(keyword)
    return keywords
