#!/usr/bin/env python3
from contextlib import closing
from typing import List, Set, Iterator

from psycopg2 import extensions

from data_access.types.active_period import ActivePeriod
from data_access.types.named_location import NamedLocation
from data_access.types.property import Property
from data_access.get_named_location_active_periods import get_active_periods
from data_access.get_named_location_properties import get_named_location_properties
from data_access.get_named_location_context import get_named_location_context
from data_access.get_named_location_site import get_named_location_site
from data_access.get_named_location_schema_name import get_named_location_schema_name


def get_named_locations(connection: extensions.connection, location_type: str) -> Iterator[NamedLocation]:
    """
    Get the named locations of the given type.

    :param connection: A database connection.
    :param location_type: The named location type.
    :return: The named locations.
    """
    sql = '''
        select
            nam_locn.nam_locn_id,
            nam_locn.nam_locn_name,
            nam_locn.nam_locn_desc,
            type.type_name
        from
            nam_locn, type
        where
            type.type_id = nam_locn.type_id
        and
            type.type_name = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [location_type])
        rows = cursor.fetchall()
        for row in rows:
            key = row[0]
            name = row[1]
            description = row[2]
            active_periods: List[ActivePeriod] = get_active_periods(connection, key)
            context: List[str] = get_named_location_context(connection, key)
            properties: List[Property] = get_named_location_properties(connection, key)
            schema_names: Set[str] = get_named_location_schema_name(connection, key)
            site: str = get_named_location_site(connection, key)
            named_location = NamedLocation(name=name, type=location_type, description=description,
                                           site=site, schema_names=schema_names, context=context,
                                           active_periods=active_periods, properties=properties)
            yield named_location
