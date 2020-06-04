#!/usr/bin/env python3
from contextlib import closing

import lib.date_formatter as date_formatter


def find_location_assets(connection, named_location_id: int):
    """
    Find assets associated with a named location.

    :param connection: A database connection.
    :type connection: database connection object
    :param named_location_id: The named location ID.
    :return: Asset UID, install and remove dates.
    """
    with closing(connection.cursor()) as cursor:
        sql = '''
            select asset_uid, install_date, remove_date 
            from is_asset_location 
            where is_asset_location.nam_locn_id = :named_location_id
        '''
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
            results.append({
                'asset_uid': asset_uid,
                'install_date': install_date,
                'remove_date': remove_date
            })
        return results


def find_all(connection):
    """
    Find all assets in the database.

    :param connection: A database connection.
    :type connection: database connection object
    :return: Asset data
    """
    with closing(connection.cursor()) as cursor:
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
        rows = cursor.execute(sql)
        results = []
        for row in rows:
            asset_uid = row[0]
            schema_name = row[1]
            results.append({
                'asset_id': asset_uid,
                'asset_type': schema_name
            })
        return results
