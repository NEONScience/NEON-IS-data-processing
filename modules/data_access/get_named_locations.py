#!/usr/bin/env python3
from contextlib import closing
from typing import Dict,List,Set,Iterator,Optional,Tuple
from datetime import datetime

from psycopg2 import extensions

from data_access.types.active_period import ActivePeriod
from data_access.types.named_location import NamedLocation
from data_access.types.property import Property
from data_access.get_named_location_active_periods import get_active_periods
from data_access.get_named_location_properties import get_named_location_properties
from data_access.get_named_location_context import get_named_location_context
from data_access.get_named_location_group import get_named_location_group
from data_access.get_named_location_parents import get_named_location_parents
from data_access.get_named_location_schema_name import get_named_location_schema_name

processingStartDate = "IS Processing Default Start Date"

def get_named_locations(connection: extensions.connection, location_type: str, source_type: str) -> Iterator[NamedLocation]:
    """
    Get the named locations of the given type.

    :param connection: A database connection.
    :param location_type: The named location type.
    :param source_type: The sensor type.
    :return: The named locations.
    """
    sql = '''
         select
             nam_locn.nam_locn_id,
             nam_locn.nam_locn_name,
             nam_locn.nam_locn_desc,
             type.type_name,
             is_sensor_type.avro_schema_name

         from
             nam_locn, type, is_asset_location, asset, is_asset_assignment, is_asset_definition, is_sensor_type
         where
             type.type_id = nam_locn.type_id
         and
             nam_locn.nam_locn_id = is_asset_location.nam_locn_id
         and
             is_asset_location.asset_uid = asset.asset_uid
         and
             asset.asset_uid = is_asset_assignment.asset_uid
         and
             is_asset_assignment.asset_definition_uuid = is_asset_definition.asset_definition_uuid
         and
             is_asset_definition.sensor_type_name = is_sensor_type.sensor_type_name
         and
             type.type_name = %s
         and
             is_sensor_type.avro_schema_name = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, (location_type, source_type))
        rows = cursor.fetchall()
        for row in rows:
            key = row[0]
            name = row[1]
            description = row[2]
            active_periods: List[ActivePeriod] = get_active_periods(connection, key)
            context: List[str] = get_named_location_context(connection, key)
            group: List[str] = get_named_location_group(connection, key)
            properties: List[Property] = get_named_location_properties(connection, key)
            schema_names: Set[str] = get_named_location_schema_name(connection, key)
            parents: Dict[str, Tuple[int, str]] = get_named_location_parents(connection, key)
            (parent_id, name) = parents['domain'] if parents else None
            domain: str = name
            (site_id, name) = parents['site'] if parents else None
            site: str = name
            processing_start_date = get_processing_start_date(properties)
            if processing_start_date is None:
                processing_start_date = get_site_start_date(connection, site_id)
                if processing_start_date is not None:
                    properties.append(Property(name=processingStartDate, value=processing_start_date))
            else:
                properties.append(Property(name=processingStartDate, value=processing_start_date))
            named_location = NamedLocation(name=name, type=location_type, description=description,
                                           domain=domain, site=site, schema_names=schema_names, context=context,
                                           group=group, active_periods=active_periods, properties=properties)
            yield named_location


def get_site_start_date(connection: extensions.connection, site_id: int) -> Optional[datetime]:
    properties: List[Property] = get_named_location_properties(connection, site_id)
    return get_processing_start_date(properties)


def get_processing_start_date(properties: List[Property]) -> Optional[datetime]:
    for prop in properties:
        name = prop.name
        if name == processingStartDate:
            return prop.value
    return None
