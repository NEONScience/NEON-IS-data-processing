"""
Module to read all file descriptions from the database.
"""
from contextlib import closing
from typing import Dict

from data_access.db_connector import DbConnector


def get_descriptions(connector: DbConnector) -> Dict[str, str]:
    """
    Get the file descriptions organized by the data product ID.

    :param connector: A database connection.
    :return: The descriptions organized by data product ID.
    """
    descriptions = {}
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'select dp_idq, description from {schema}.pub_table_def'
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            data_product_id: str = row[0]
            description: str = row[1]
            descriptions[data_product_id] = description
    return descriptions
