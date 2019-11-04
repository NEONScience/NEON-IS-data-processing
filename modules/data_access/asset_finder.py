from contextlib import closing

import lib.date_formatter as date_formatter


def find_location_assets(connection, named_location_id):
    with closing(connection.cursor()) as cursor:
        sql = '''
            select 
                asset_uid, install_date, remove_date 
            from is_asset_location 
            where is_asset_location.nam_locn_id = :named_location_id
        '''
        cursor.prepare(sql)
        rows = cursor.execute(None, named_location_id=named_location_id)
        results = []
        for row in rows:
            asset_uid = row[0]
            install_date = date_formatter.convert(row[1])
            remove_date = date_formatter.convert(row[2])
            results.append({
                'asset_uid': asset_uid,
                'install_date': install_date,
                'remove_date': remove_date
            })
        return results


def find_all(connection):
    """
    Find all assets in the database.
    :return: Dictionary of asset data
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
            '''
        rows = cursor.execute(sql)
        results = []
        for row in rows:
            asset_uid = row[0]
            avro_schema_name = row[1]
            results.append({
                'asset_id': asset_uid,
                'asset_type': avro_schema_name
            })
        return results
