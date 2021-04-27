#!/usr/bin/env python3
from contextlib import closing
from typing import List

from psycopg2 import extensions

import common.date_formatter as date_formatter
from data_access.types.property import Property


def get_named_location_properties(connection: extensions.connection, named_location_id: int) -> List[Property]:
    """
    Get the properties associated with a named location.

    :param connection: A database connection.
    :param named_location_id: The named location ID to search.
    :return: The named location properties.
    """
    sql = '''
        select
            attr.attr_name,
            property.string_value,
            property.number_value,
            property.date_value
        from
            property
        join
            attr on property.attr_id = attr.attr_id
        where
            property.nam_locn_id = %s
    '''
    properties: List[Property] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        rows = cursor.fetchall()
        for row in rows:
            name = row[0]
            string_value = row[1]
            number_value = row[2]
            date_value = row[3]
            if string_value is not None:
                properties.append(Property(name=name, value=string_value))
            if number_value is not None:
                if(name == 'Required Asset Management Location ID'):
                    properties.append(Property(name=name, value=int(number_value)))
                else:
                    properties.append(Property(name=name, value=number_value))
            if date_value is not None:
                date_value = date_formatter.to_string(date_value)
                properties.append(Property(name=name, value=date_value))
    return properties
