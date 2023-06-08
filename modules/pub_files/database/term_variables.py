from contextlib import closing
from typing import NamedTuple

from psycopg2.extras import DictCursor

from data_access.db_connector import DbConnector


class TermVariables(NamedTuple):
    description: str
    download_package: str
    publication_format: str
    data_type: str
    units: str


def get_term_variables(connector: DbConnector, data_product: str, term_name: str) -> TermVariables:
    """
    Returns the variables for the data product and term name.

    :param connector: A connector object.
    :param data_product: The data product, example form: NEON.DOM.SITE.DP1.00098.001.00762.HOR.VER.001.
    :param term_name: The term name.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
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
            dp_number = '{data_product}'
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
