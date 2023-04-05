from contextlib import closing
from typing import Optional

from data_access.db_connector import DbConnector


def get_unit_eml_type(connector: DbConnector, unit_name: str) -> Optional[str]:
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'select eml_attr from {schema}.unit where unit_name = %s'
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [unit_name])
        row = cursor.fetchone()
        eml_attribute = row[0]
        if eml_attribute == 'EMLUnits':
            return 'standard'
        elif eml_attribute == 'Custom':
            return 'custom'
        return None
