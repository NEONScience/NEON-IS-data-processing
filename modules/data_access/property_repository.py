#!/usr/bin/env python3
from contextlib import closing

import structlog

import common.date_formatter as date_formatter

log = structlog.get_logger()


class PropertyRepository(object):
    """Class to represent a property repository backed by a database."""

    def __init__(self, connection):
        self.connection = connection

    def get_named_location_properties(self, named_location_id: int):
        """
        Get the properties associated with a named location as name:value pairs.

        :param named_location_id: The named location ID to search.
        :return: The named location properties.
        """
        sql = '''
            select
                attr.attr_name,
                attr.attr_desc,
                property.string_value,
                property.number_value,
                property.date_value
            from
                property
            join
                attr on property.attr_id = attr.attr_id
            where
                property.nam_locn_id = :named_location_id
        '''
        with closing(self.connection.cursor()) as cursor:
            rows = cursor.execute(sql, named_location_id=named_location_id)
            properties = []
            for row in rows:
                name = row[0]
                string_value = row[2]
                number_value = row[3]
                date_value = row[4]
                if string_value is not None:
                    properties.append({name: string_value})
                if number_value is not None:
                    properties.append({name: number_value})
                if date_value is not None:
                    date_value = date_formatter.convert(date_value)
                    properties.append({name: date_value})
            return properties
