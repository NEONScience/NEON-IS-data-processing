from contextlib import closing
from typing import Optional

from data_access.db_connector import DbConnector


class EmlUnitType:

    standard = 'standard'
    custom = 'custom'

    def __init__(self, unit_type: str):
        self.unit_type = unit_type

    def is_standard(self):
        return self.unit_type == self.standard

    def is_custom(self):
        return self.unit_type == self.custom


def get_unit_eml_type(connector: DbConnector, unit_name: str) -> Optional[EmlUnitType]:
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'select eml_attr from {schema}.unit where unit_name = %s'
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [unit_name])
        row = cursor.fetchone()
        eml_attribute = row[0]
        if eml_attribute == 'EMLUnits':
            return EmlUnitType(EmlUnitType.standard)
        elif eml_attribute == 'Custom':
            return EmlUnitType(EmlUnitType.custom)
        return None
