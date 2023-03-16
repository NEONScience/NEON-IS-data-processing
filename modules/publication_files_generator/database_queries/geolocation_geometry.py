"""Read a location's geometry from the database."""
from contextlib import closing
from typing import NamedTuple

from data_access.db_connector import DbConnector


class Coordinates(NamedTuple):
    latitude: str
    longitude: str
    elevation: str


def get_geometry(connector: DbConnector, named_location_name: str) -> str:
    """
    Get the geometry for the site.

    :param connector: A database connection.
    :param named_location_name: The named location name.
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
        cursor.execute(sql, [named_location_name])
        row = cursor.fetchone()
        geometry: str = row[0]
    return geometry


def get_coordinates(geometry: str) -> Coordinates:
    # POINT Z (-104.745591 40.815536 1653.9151)
    if geometry.startswith('POINT'):
        coordinates = geometry.split('(')[1].replace(')', '')
    elif geometry.startswith('POLYGON'):
        # POLYGON Z ((-104.746013 40.815892 1654.009392,-104.745973 40.815922 1654.052064, ...))
        coordinates = geometry.split('((')[1].replace('))', '')
    else:
        raise Exception(f'Location geometry {geometry} is not point or polygon.')
    parts = coordinates.split(' ')
    longitude = parts[0]
    latitude = parts[1]
    elevation = parts[2]
    return Coordinates(latitude=latitude, longitude=longitude, elevation=elevation)


def get_formatted_coordinates(geometry: str) -> str:
    coordinates = get_coordinates(geometry)
    return f'{coordinates.latitude} {coordinates.longitude} WGS 84'
