from contextlib import closing
from typing import NamedTuple, Callable

from psycopg2.extras import DictCursor

from data_access.db_connector import DbConnector
from pub_files.data_product import get_term_data_product_number


class TermVariables(NamedTuple):
    description: str
    download_package: str
    publication_format: str
    data_type: str
    units: str


def make_get_term_variables(connector: DbConnector) -> Callable[[str, str], TermVariables]:
    """Closure to hide the connector from the returned function's callers."""

    def get_term_variables(data_product_id: str, term_name: str) -> TermVariables:
        """Returns the variables for the term name."""
        connection = connector.get_connection()
        schema = connector.get_schema()
        data_product_number = get_term_data_product_number(data_product_id)
        sql = f'''
            select
                description,
                download_package,
                pub_format,
                data_type_code,
                unit_name
            from
                {schema}.pub_field_def
            where
                field_name = '{term_name}'
            and 
                dp_number = '{data_product_number}'
        '''
        with closing(connection.cursor(cursor_factory=DictCursor)) as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            description = row['description']
            download_package = row['download_package']
            publication_format = row['pub_format']
            data_type = row['data_type_code']
            units = row['unit_name']
            term_variables = TermVariables(description=description,
                                           download_package=download_package,
                                           publication_format=publication_format,
                                           data_type=data_type,
                                           units=units)
        return term_variables

    return get_term_variables
