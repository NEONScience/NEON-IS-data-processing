from contextlib import closing
from typing import Callable

from data_access.db_connector import DbConnector


def make_get_term_name(connector: DbConnector) -> Callable[[str], str]:
    """Closure to hide connector from clients."""

    def get_term_name(term_number: str) -> str:
        """Returns the term name for the given term number."""
        connection = connector.get_connection()
        schema = connector.get_schema()
        sql = f'select term_name from {schema}.term where term_number = %s'
        with closing(connection.cursor()) as cursor:
            cursor.execute(sql, [term_number])
            row = cursor.fetchone()
            term_name = row[0]
        return term_name

    return get_term_name
