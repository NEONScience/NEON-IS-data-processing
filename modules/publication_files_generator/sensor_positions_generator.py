import csv
import math
from datetime import datetime
from pathlib import Path
from typing import List, Callable, Tuple

import structlog

from common import date_formatter
from publication_files_generator.database_queries.geolocation_geometry import Coordinates, get_coordinates
from publication_files_generator.database_queries.named_location import NamedLocation
from publication_files_generator.database_queries.sensor_geolocations import GeoLocation
from publication_files_generator.filename_formatter import get_filename

log = structlog.get_logger()

FILE_COLUMNS = ['HOR.VER',
           'sensorLocationID', 'sensorLocationDescription',
           'positionStartDateTime', 'positionEndDateTime',
           'referenceLocationID', 'referenceLocationIDDescription',
           'referenceLocationIDStartDateTime', 'referenceLocationIDEndDateTime',
           'xOffset', 'yOffset', 'zOffset',
           'pitch', 'roll', 'azimuth',
           'locationReferenceLatitude', 'locationReferenceLongitude', 'locationReferenceElevation',
           'eastOffset', 'northOffset',
           'xAzimuth', 'yAzimuth']


def get_indices(named_location: NamedLocation) -> Tuple[str, str]:
    properties = named_location.properties
    horizontal_index = ''
    vertical_index = ''
    for prop in properties:
        if prop.name == 'HOR':
            horizontal_index = prop.value
        if prop.name == 'VER':
            vertical_index = prop.value
    return horizontal_index, vertical_index


def get_azimuth_values(geolocation: GeoLocation):
    x_azimuth = 0
    y_azimuth = 0
    for prop in geolocation.properties:
        if prop.name == 'x Azimuth Angle':
            x_azimuth = prop.value
        if prop.name == 'y Azimuth Angle':
            y_azimuth = prop.value
    return x_azimuth, y_azimuth


def generate_positions_file(out_path: Path,
                            locations_path: Path,
                            location_path_index: int,
                            domain: str,
                            site: str,
                            year: str,
                            month: str,
                            data_product_id: str,
                            timestamp: datetime,
                            get_geolocations: Callable[[str], List[GeoLocation]],
                            get_named_location: Callable[[str], NamedLocation],
                            get_geometry: Callable[[str], str]) -> str:
    file_root = Path(out_path, site, year, month)
    file_root.mkdir(parents=True, exist_ok=True)
    filename = get_filename(domain=domain, site=site, data_product_id=data_product_id, timestamp=timestamp,
                            file_type='sensor_positions', extension='csv')
    file_path = Path(file_root, filename)
    with open(file_path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(FILE_COLUMNS)
        for path in locations_path.glob('*'):
            named_location_name = path.parts[location_path_index]
            named_location = get_named_location(named_location_name)
            (horizontal_index, vertical_index) = get_indices(named_location)
            row_hor_ver = f'{horizontal_index}.{vertical_index}'
            row_location_id = named_location.location_id
            row_description = named_location.description
            for geolocation in get_geolocations(named_location_name):
                start_date = geolocation.start_date
                if start_date is not None:
                    row_start = date_formatter.to_string(start_date)
                else:
                    row_start = ''
                end_date = geolocation.end_date
                if end_date is not None:
                    row_end = date_formatter.to_string(end_date)
                else:
                    row_end = ''
                row_x_offset = str(geolocation.x_offset)
                row_y_offset = str(geolocation.y_offset)
                row_z_offset = str(geolocation.z_offset)
                row_pitch = str(geolocation.alpha)
                row_roll = str(geolocation.beta)
                row_azimuth = str(geolocation.gamma)
                reference_location_name = geolocation.offset_name
                reference_location = get_named_location(reference_location_name)
                row_reference_location_id = reference_location.location_id
                row_reference_location_description = geolocation.offset_description
                reference_location_coordinates: Coordinates = get_coordinates(get_geometry(reference_location_name))
                row_reference_location_latitude = reference_location_coordinates.latitude
                row_reference_location_longitude = reference_location_coordinates.longitude
                row_reference_location_elevation = reference_location_coordinates.elevation
                (x_azimuth, y_azimuth) = get_azimuth_values(geolocation)
                row_x_azimuth = str(x_azimuth)
                row_y_azimuth = str(y_azimuth)
                (east_offset, north_offset) = calculate_offsets(x_azimuth, y_azimuth,
                                                                geolocation.x_offset, geolocation.y_offset)
                row_east_offset = str(east_offset)
                row_north_offset = str(north_offset)
                for reference_geolocation in get_geolocations(geolocation.offset_name):
                    reference_start_date = reference_geolocation.start_date
                    reference_end_date = reference_geolocation.end_date
                    if reference_start_date is not None:
                        row_reference_start = date_formatter.to_string(reference_start_date)
                    else:
                        row_reference_start = ''
                    if reference_end_date is not None:
                        row_reference_end = date_formatter.to_string(reference_end_date)
                    else:
                        row_reference_end = ''
                    row = [row_hor_ver,
                           row_location_id,
                           row_description,
                           row_start,
                           row_end,
                           row_reference_location_id,
                           row_reference_location_description,
                           row_reference_start,
                           row_reference_end,
                           row_x_offset,
                           row_y_offset,
                           row_z_offset,
                           row_pitch,
                           row_roll,
                           row_azimuth,
                           row_reference_location_latitude,
                           row_reference_location_longitude,
                           row_reference_location_elevation,
                           row_east_offset,
                           row_north_offset,
                           row_x_azimuth,
                           row_y_azimuth]
                    writer.writerow(row)
    return filename


def calculate_offsets(x_azimuth, y_azimuth, x_offset, y_offset):
    """Calculate east and north offsets."""
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
    # convert to polar coordinates
    radius = math.sqrt(x_offset * x_offset + y_offset * y_offset)
    if x_offset == 0:
        theta = 90.
    else:
        theta = math.degrees(math.atan(y_offset / x_offset))
    # quadrant correction
    if x_offset < 0:
        theta += 180
    if x_offset > 0 > y_offset:
        theta += 360
    # rotate by azimuth
    cardinal_theta = theta - corrected_y_azimuth
    east_offset = radius * math.cos(math.radians(cardinal_theta))
    north_offset = radius * math.sin(math.radians(cardinal_theta))
    return east_offset, north_offset
