#!/usr/bin/env python3
from contextlib import closing

from data_access.db_connector import DbConnector


def get_geometry(connector: DbConnector, location_name: str) -> str:
    """
    Get the geometry for the site.

    :param connector: A database connection.
    :param location_name: The location name.
    :return: The geometry.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            ST_AsText(locn_geom)
        from 
            {schema}.locn, {schema}.locn_nam_locn, {schema}.nam_locn 
        where   
            {schema}.locn_nam_locn.locn_id = {schema}.locn.locn_id
        and 
            {schema}.nam_locn.nam_locn_id = {schema}.locn_nam_locn.nam_locn_id 
        and 
            {schema}.nam_locn.nam_locn_name = %s
    '''
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [location_name])
        row = cursor.fetchone()
        geometry: str = row[0]
    return geometry


def get_point_coordinates(geometry: str) -> str:
    """Parse coordinates from the geometry string format 'POINT Z (-104.745591 40.815536 1653.9151)'"""
    coordinates = geometry.split('(')[1].replace(')', '')
    parts = coordinates.split(' ')
    longitude = parts[0]
    latitude = parts[1]
    return f'{latitude} {longitude} WGS 84'
