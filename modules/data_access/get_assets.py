#!/usr/bin/env python3
from contextlib import closing
from typing import Iterator

from psycopg2 import extensions

from data_access.types.asset import Asset


def get_assets(connection: extensions.connection) -> Iterator[Asset]:
    """
    Get assets.

    :param connection: A database connection.
    :return: The assets.
    """
    sql = '''
         select
             asset.asset_uid,
             is_sensor_type.avro_schema_name
         from
             asset, is_asset_assignment, is_asset_definition, is_sensor_type
         where
             asset.asset_uid = is_asset_assignment.asset_uid
         and
             is_asset_assignment.asset_definition_uuid = is_asset_definition.asset_definition_uuid
         and
             is_asset_definition.sensor_type_name = is_sensor_type.sensor_type_name
         and 
             is_sensor_type.avro_schema_name is not null
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            asset_id = row[0]
            asset_type = row[1]
            yield Asset(id=asset_id, type=asset_type)
