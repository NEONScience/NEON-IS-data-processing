import math
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

from common import date_formatter
from data_access.db_connector import DbConnector
from data_access.get_geolocation_properties import get_geolocation_properties
from data_access.types.property import Property


@dataclass(frozen=True)
class GeoLocation:
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
    offset_id: int
    offset_name: str
    offset_description: str
    properties: List[Property]

    def get_dates(self) -> Tuple[str, str]:
        start_date = self.start_date
        if start_date is not None:
            start = date_formatter.to_string(start_date)
        else:
            start = ''
        end_date = self.end_date
        if end_date is not None:
            end = date_formatter.to_string(end_date)
        else:
            end = ''
        return start, end

    def get_offsets(self) -> Tuple[float, float]:
        """Calculate east and north offsets."""
        # convert to polar coordinates
        radius = math.sqrt(self.x_offset * self.x_offset + self.y_offset * self.y_offset)
        theta = self._get_theta()
        corrected_y_azimuth = self._correct_y_azimuth()
        cardinal_theta = theta - corrected_y_azimuth
        east_offset = radius * math.cos(math.radians(cardinal_theta))
        north_offset = radius * math.sin(math.radians(cardinal_theta))
        return east_offset, north_offset

    def get_azimuth_values(self) -> Tuple[float, float]:
        x_azimuth = 0
        y_azimuth = 0
        for prop in self.properties:
            if prop.name == 'x Azimuth Angle':
                x_azimuth = float(prop.value)
            if prop.name == 'y Azimuth Angle':
                y_azimuth = float(prop.value)
        return x_azimuth, y_azimuth

    def _correct_y_azimuth(self) -> float:
        (x_azimuth, y_azimuth) = self.get_azimuth_values()
        corrected_y_azimuth = y_azimuth
        if y_azimuth < x_azimuth:
            diff = x_azimuth - y_azimuth
        else:
            diff = 360 - x_azimuth + y_azimuth
        if diff > 90:
            delta = diff - 90
            corrected_y_azimuth = 0.5 * delta + y_azimuth
            if corrected_y_azimuth >= 360:
                corrected_y_azimuth -= 360
        if diff < 90:
            delta = 90 - diff
            corrected_y_azimuth = y_azimuth - 0.5 * delta
            if corrected_y_azimuth < 0:
                corrected_y_azimuth += 360
        return corrected_y_azimuth

    def _get_theta(self) -> float:
        if self.x_offset == 0:
            theta = 90.
        else:
            theta = math.degrees(math.atan(self.y_offset / self.x_offset))
        # quadrant correction
        if self.x_offset < 0:
            theta += 180
        if self.x_offset > 0 > self.y_offset:
            theta += 360
        return theta


def get_geolocations(connector: DbConnector, named_location: str) -> List[GeoLocation]:
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
            nam_locn_id_off
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
    geolocations = []
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
            offset_id = row[10]
            properties = get_geolocation_properties(connector, location_id)
            (offset_name, offset_description) = get_description(connector, offset_id)
            geolocation = GeoLocation(location_id=location_id,
                                      geometry=geometry,
                                      start_date=start_date,
                                      end_date=end_date,
                                      alpha=alpha,
                                      beta=beta,
                                      gamma=gamma,
                                      x_offset=x_offset,
                                      y_offset=y_offset,
                                      z_offset=z_offset,
                                      offset_id=offset_id,
                                      offset_name=offset_name,
                                      offset_description=offset_description,
                                      properties=properties)
            geolocations.append(geolocation)
    return geolocations


def get_description(connector: DbConnector, named_location_id: str) -> Tuple[str, str]:
    """Get a named location name and description using the named location ID."""
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'select nam_locn_name, nam_locn_desc from {schema}.nam_locn where nam_locn_id = %s'
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [named_location_id])
        row = cursor.fetchone()
        name = row[0]
        description = row[1]
        return name, description
