from contextlib import closing
from dataclasses import dataclass
from typing import List, Tuple

from data_access.types.property import Property
from data_access.get_named_location_properties import get_named_location_properties
from data_access.db_connector import DbConnector


@dataclass(frozen=True)
class NamedLocation:
    location_id: int
    name: str
    description: str
    properties: List[Property]

    def get_indices(self) -> Tuple[str, str]:
        """Returns the horizontal and vertical indices."""
        properties = self.properties
        horizontal_index = ''
        vertical_index = ''
        for prop in properties:
            if prop.name == 'HOR':
                horizontal_index = prop.value
            if prop.name == 'VER':
                vertical_index = prop.value
        return horizontal_index, vertical_index


def get_named_location(connector: DbConnector, named_location_name: str) -> NamedLocation:
    """
    Get the named location.

    :param connector: A database connection.
    :param named_location_name: The named location name.
    :return: The named location.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
         select
             nam_locn_id,
             nam_locn_name,
             nam_locn_desc
         from
             {schema}.nam_locn
         where
             nam_locn_name = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_name])
        row = cursor.fetchone()
        location_id = row[0]
        name = row[1]
        description = row[2]
        properties: List[Property] = get_named_location_properties(connector, location_id)
        return NamedLocation(location_id=location_id, name=name, description=description, properties=properties)
