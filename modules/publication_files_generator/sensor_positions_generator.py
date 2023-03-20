import csv
import math
from datetime import datetime
from pathlib import Path
from typing import List, Callable, Tuple

from common import date_formatter
from publication_files_generator.database_queries.geolocation_geometry import get_coordinates
from publication_files_generator.database_queries.named_location import NamedLocation
from publication_files_generator.database_queries.sensor_geolocations import GeoLocation
from publication_files_generator.filename_formatter import get_filename


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


def get_dates(geolocation: GeoLocation) -> Tuple[str, str]:
    start_date = geolocation.start_date
    if start_date is not None:
        start = date_formatter.to_string(start_date)
    else:
        start = ''
    end_date = geolocation.end_date
    if end_date is not None:
        end = date_formatter.to_string(end_date)
    else:
        end = ''
    return start, end


def get_azimuth_values(geolocation: GeoLocation) -> Tuple[float, float]:
    x_azimuth = 0
    y_azimuth = 0
    for prop in geolocation.properties:
        if prop.name == 'x Azimuth Angle':
            x_azimuth = float(prop.value)
        if prop.name == 'y Azimuth Angle':
            y_azimuth = float(prop.value)
    return x_azimuth, y_azimuth


def generate_positions_file(out_path: Path,
                            location_path: Path,
                            domain: str,
                            site: str,
                            year: str,
                            month: str,
                            data_product_id: str,
                            timestamp: datetime,
                            get_geolocations: Callable[[str], List[GeoLocation]],
                            get_named_location: Callable[[str], NamedLocation],
                            get_geometry: Callable[[str], str]) -> str:
    root = Path(out_path, site, year, month)
    root.mkdir(parents=True, exist_ok=True)
    filename = get_filename(domain=domain, site=site, data_product_id=data_product_id, timestamp=timestamp,
                            file_type='sensor_positions', extension='csv')
    with open(Path(root, filename), 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(FILE_COLUMNS)
        for path in location_path.glob('*.json'):
            if path.is_file() and path.name.startswith('CFGLOC'):
                named_location_name = path.stem
                named_location = get_named_location(named_location_name)
                (horizontal_index, vertical_index) = get_indices(named_location)
                row_hor_ver = f'{horizontal_index}.{vertical_index}'
                row_location_id = named_location.location_id
                row_description = named_location.description
                for geolocation in get_geolocations(named_location_name):
                    reference_location_coordinates = get_coordinates(get_geometry(geolocation.offset_name))
                    (x_azimuth, y_azimuth) = get_azimuth_values(geolocation)
                    (east_offset, north_offset) = calculate_offsets(x_azimuth, y_azimuth,
                                                                    geolocation.x_offset, geolocation.y_offset)
                    (row_position_start_date, row_position_end_date) = get_dates(geolocation)
                    row_x_offset: float = round(geolocation.x_offset, 2)
                    row_y_offset: float = round(geolocation.y_offset, 2)
                    row_z_offset: float = round(geolocation.z_offset, 2)
                    row_pitch: float = round(geolocation.alpha, 2)
                    row_roll: float = round(geolocation.beta, 2)
                    row_azimuth: float = round(geolocation.gamma, 2)
                    row_reference_location_id: int = (get_named_location(geolocation.offset_name)).location_id
                    row_reference_location_description: str = geolocation.offset_description
                    row_reference_location_latitude: float = round(reference_location_coordinates.latitude, 6)
                    row_reference_location_longitude: float = round(reference_location_coordinates.longitude, 6)
                    row_reference_location_elevation: float = round(reference_location_coordinates.elevation, 2)
                    row_x_azimuth: float = round(x_azimuth, 2)
                    row_y_azimuth: float = round(y_azimuth, 2)
                    row_east_offset: float = round(east_offset, 2)
                    row_north_offset: float = round(north_offset, 2)
                    for reference_geolocation in get_geolocations(geolocation.offset_name):
                        (row_reference_location_start_date,
                         row_reference_location_end_date) = get_dates(reference_geolocation)
                        row = [row_hor_ver,
                               row_location_id,
                               row_description,
                               row_position_start_date,
                               row_position_end_date,
                               row_reference_location_id,
                               row_reference_location_description,
                               row_reference_location_start_date,
                               row_reference_location_end_date,
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
