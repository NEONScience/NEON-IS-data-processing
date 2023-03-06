"""
Module to read all file descriptions from the database.
"""
from contextlib import closing
from typing import Dict

from data_access.db_connector import DbConnector


def remove_prefix(data_product_idq: str):
    return data_product_idq.replace('NEON.DOM.SITE.', '')


def get_descriptions(connector: DbConnector) -> Dict[str, str]:
    """
    Get the file descriptions organized by the data product idq.

    :param connector: A database connection.
    :return: The descriptions organized by idq.
    """
    descriptions = {}
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'select dp_idq, description from {schema}.pub_table_def'
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            dp_idq: str = remove_prefix(row[0])
            description: str = row[1]
            descriptions[dp_idq] = description
    return descriptions
