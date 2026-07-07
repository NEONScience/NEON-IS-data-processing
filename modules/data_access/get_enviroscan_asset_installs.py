#!/usr/bin/env python3
from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector
from data_access.types.asset_install import AssetInstall


def get_enviroscan_asset_installs(connector: DbConnector) -> List[AssetInstall]:
    """
    Return every (CFGLOC × asset install period) for enviroscan probes across all sites and time.

    Corresponds to Q1 in swc_loc_depths_notes.md. `DISTINCT` prevents the Cartesian
    that arises when an asset has multiple rows in `is_asset_assignment`.

    :param connector: A database connection.
    :return: One entry per (CFGLOC, asset_uid, install period).
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select distinct
            nl.nam_locn_name,
            nl.nam_locn_desc,
            nl.nam_locn_id,
            ial.asset_uid,
            ial.install_date,
            ial.remove_date
        from {schema}.is_asset_location    ial
        join {schema}.nam_locn             nl   on ial.nam_locn_id = nl.nam_locn_id
        join {schema}.is_asset_assignment  asgn on ial.asset_uid = asgn.asset_uid
        join {schema}.is_asset_definition  def  on def.asset_definition_uuid = asgn.asset_definition_uuid
        where def.sensor_type_name = 'enviroscan'
        order by nl.nam_locn_name, ial.install_date desc
    '''
    installs: List[AssetInstall] = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        for row in cursor.fetchall():
            installs.append(AssetInstall(
                cfgloc=row[0],
                cfgloc_description=row[1],
                nam_locn_id=row[2],
                asset_uid=row[3],
                install_date=row[4],
                remove_date=row[5],
            ))
    return installs
