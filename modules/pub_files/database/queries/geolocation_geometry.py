from contextlib import closing

from data_access.db_connector import DbConnector
from pub_files.geometry import Geometry


def get_geometry(connector: DbConnector, named_location_name: str) -> Geometry:
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            ST_AsText(locn_geom),
            ST_SRID(locn_geom)
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
        srid: int = row[1]
    return Geometry(geometry=geometry, srid=srid)