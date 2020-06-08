#!/usr/bin/env python3
from contextlib import closing

import common.date_formatter as date_formatter


class AssetRepository(object):
    """Class to represent an asset repository backed by a database."""

    def __init__(self, connection):
        self.connection = connection

    def get_all(self):
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
            assets = []
            for row in rows:
                asset_uid = row[0]
                schema_name = row[1]
                assets.append({'asset_id': asset_uid, 'asset_type': schema_name})
            return assets

    def get_location_assets(self, named_location_id: int):
        """
        Get assets associated with a named location.

        :param named_location_id: The named location ID.
        :return: Asset UID, install and remove dates.
        """
        sql = '''
            select asset_uid, install_date, remove_date 
            from is_asset_location 
            where is_asset_location.nam_locn_id = :named_location_id
        '''
        with closing(self.connection.cursor()) as cursor:
            cursor.prepare(sql)
            rows = cursor.execute(None, named_location_id=named_location_id)
            results = []
            for row in rows:
                asset_uid = row[0]
                install_date = row[1]
                remove_date = row[2]
                if install_date is not None:
                    install_date = date_formatter.convert(install_date)
                if remove_date is not None:
                    remove_date = date_formatter.convert(remove_date)
                results.append({'asset_uid': asset_uid, 'install_date': install_date, 'remove_date': remove_date})
            return results
