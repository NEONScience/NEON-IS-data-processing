from contextlib import closing
from datetime import datetime
from typing import NamedTuple, List

from data_access.db_connector import DbConnector
from data_access.get_geolocation_properties import get_geolocation_properties
from data_access.types.property import Property


class Location(NamedTuple):
    location_id: int
    geometry: str
    start_date: datetime
    end_date: datetime
    alpha: float
    beta: float
    gamma: float
    x_offset: float
    y_offset: float
    z_offset: float
    named_location_offset_id: int
    named_location_offset_name: str
    location_properties: List[Property]


def get_locations(connector: DbConnector, named_location: str) -> List[Location]:
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select
            locn.locn_id,
            ST_AsText(locn_geom) as geo,
            locn_nam_locn_strt_date, 
            locn_nam_locn_end_date, 
            locn_alph_ortn, 
            locn_beta_ortn, 
            locn_gama_ortn, 
            locn_x_off, 
            locn_y_off, 
            locn_z_off, 
            nam_locn_id_off,
            nam_locn_name
        from 
            {schema}.locn
        join 
            {schema}.locn_nam_locn 
        on 
            locn.locn_id = locn_nam_locn.locn_id
        join 
            {schema}.nam_locn 
        on 
            locn_nam_locn.nam_locn_id = nam_locn.nam_locn_id 
        and 
            nam_locn.nam_locn_name = %s
    '''
    locations = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location])
        rows = cursor.fetchall()
        for row in rows:
            location_id = row[0]
            geometry = row[1]
            start_date = row[2]
            end_date = row[3]
            alpha = float(row[4])
            beta = float(row[5])
            gamma = float(row[6])
            x_offset = float(row[7])
            y_offset = float(row[8])
            z_offset = float(row[9])
            named_location_offset_id = row[10]
            named_location_offset_name = row[11]
            location_properties = get_geolocation_properties(connector, location_id)
            location = Location(
                location_id=location_id,
                geometry=geometry,
                start_date=start_date,
                end_date=end_date,
                alpha=alpha,
                beta=beta,
                gamma=gamma,
                x_offset=x_offset,
                y_offset=y_offset,
                z_offset=z_offset,
                named_location_offset_id=named_location_offset_id,
                named_location_offset_name=named_location_offset_name,
                location_properties=location_properties)
            locations.append(location)
    return locations
