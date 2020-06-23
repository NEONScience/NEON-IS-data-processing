#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.asset import Asset


class AssetRepository(object):
    """Class to represent an asset repository backed by a database."""

    def __init__(self, connection):
        self.connection = connection

    def get_assets(self) -> List[Asset]:
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
        with closing(self.connection.cursor()) as cursor:
            rows = cursor.execute(sql)
            assets: List[Asset] = []
            for row in rows:
                asset_uid = row[0]
                schema_name = row[1]
                assets.append(Asset(id=asset_uid, type=schema_name))
            return assets
