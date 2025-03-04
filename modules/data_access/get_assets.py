#!/usr/bin/env python3
from contextlib import closing
from typing import Iterator, Set

from data_access.types.asset import Asset
from data_access.db_connector import DbConnector


def get_assets(connector: DbConnector, source_type: str) -> Iterator[Asset]:
    """
    Get assets for source_type.

    :param connector: A database connection.
    :param source_type: The type of sensor.
    :return: The assets.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
         select
             DISTINCT
             asset.asset_uid,
             is_sensor_type.avro_schema_name
         from
             {schema}.asset, 
             {schema}.is_asset_assignment, 
             {schema}.is_asset_definition, 
             {schema}.is_sensor_type
         where
             asset.asset_uid = is_asset_assignment.asset_uid
         and
             is_asset_assignment.asset_definition_uuid = is_asset_definition.asset_definition_uuid
         and
             is_asset_definition.sensor_type_name = is_sensor_type.sensor_type_name
         and 
             is_sensor_type.avro_schema_name = %s
         order by asset_uid 
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [source_type])
        rows = cursor.fetchall()
        for row in rows:
            asset_id = row[0]
            asset_type = row[1]
            yield Asset(id=asset_id, type=asset_type)


def get_asset_definition_by_date(connector: DbConnector, install_date: str, remove_date: str, asset_id: int) -> Set[Asset]:
    """
        Get asset definition by installation date.

        :param connector: A database connection.
        :param install_date: The install date of sensor.
        :param remove_date: The remove date of sensor.
        :param asset_id: sensor id (asset_uid).
        :return: The asset with asset definition (model_number, manufacturer, software_version).
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
             select
                 is_asset_definition.sensor_type_name,
                 is_asset_definition.model_number,
                 is_asset_definition.manufacturer_name,
                 is_asset_definition.sw_version
             from
                 {schema}.asset, 
                 {schema}.is_asset_assignment, 
                 {schema}.is_asset_definition
             where
                 asset.asset_uid = is_asset_assignment.asset_uid
             and
                 is_asset_assignment.asset_definition_uuid = is_asset_definition.asset_definition_uuid
             and
                 is_asset_assignment.start_date < %s
             and
                 (is_asset_assignment.end_date is null or is_asset_assignment.end_date > %s)
             and
                 asset.asset_uid = %s
    '''
    assets = set()
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, (install_date, remove_date, asset_id))
        rows = cursor.fetchall()
        for row in rows:
            asset_type = row[0]
            asset_model = row[1]
            asset_manufacturer = row[2]
            asset_software = row[3]
            asset = Asset(id=asset_id, type=asset_type, model=asset_model,
                          manufacturer=asset_manufacturer, software_version=asset_software)
            assets.add(asset)
        return assets
