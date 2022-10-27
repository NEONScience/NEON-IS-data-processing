#!/usr/bin/env python3
from contextlib import closing
from typing import List

import common.date_formatter as date_formatter
from data_access.types.property import Property
from data_access.db_connector import DbConnector


def get_geolocation_properties(connector: DbConnector, geolocation_id: int) -> List[Property]:
    """
    Get the properties associated with a geolocation.

    :param connector: A database connection.
    :param geolocation_id: The geolocation ID to search.
    :return: The geolocation properties.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            attr.attr_name,
            property.string_value,
            property.number_value,
            property.date_value
        from
            {schema}.property
        join
            {schema}.attr on property.attr_id = attr.attr_id
        where
            property.locn_id = %s
    '''
    properties: List[Property] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [geolocation_id])
        rows = cursor.fetchall()
        for row in rows:
            name = row[0]
            string_value = row[1]
            number_value = row[2]
            date_value = row[3]
            if string_value is not None:
                properties.append(Property(name=name, value=string_value))
            if number_value is not None:
                properties.append(Property(name=name, value=float(number_value)))
            if date_value is not None:
                date_value = date_formatter.to_string(date_value)
                properties.append(Property(name=name, value=date_value))
    return properties
