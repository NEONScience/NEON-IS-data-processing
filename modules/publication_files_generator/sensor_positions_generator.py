import math
from datetime import datetime
from pathlib import Path
from typing import List, Callable, NamedTuple

from common import date_formatter
from publication_files_generator.database_queries.geolocation_geometry import Coordinates, get_coordinates
from publication_files_generator.database_queries.named_location import NamedLocation
from publication_files_generator.database_queries.sensor_geolocations import GeoLocation
from publication_files_generator.filename_formatter import get_filename

COLUMN_NAMES = ['HOR.VER',
                'name',
                'description',
                'start',
                'end',
                'referenceName',
                'referenceDescription',
                'referenceStart',
                'referenceEnd',
                'xOffset',
                'yOffset',
                'zOffset',
                'pitch',
                'roll',
                'azimuth',
                'referenceLatitude',
                'referenceLongitude',
                'referenceElevation',
                'eastOffset',
                'northOffset',
                'xAzimuth',
                'yAzimuth']


class NamedLocationFields(NamedTuple):
    description: str
    horizontal_index: str
    vertical_index: str


def get_named_location_fields(get_named_location: Callable[[str], NamedLocation],
                              named_location_name: str) -> NamedLocationFields:
    named_location = get_named_location(named_location_name)
    description = named_location.description
    properties = named_location.properties
    horizontal_index = ''
    vertical_index = ''
    for prop in properties:
        if prop.name == 'HOR':
            horizontal_index = prop.value
        if prop.name == 'VER':
            vertical_index = prop.value
    return NamedLocationFields(description=description,
                               horizontal_index=horizontal_index,
                               vertical_index=vertical_index)


def find_azimuth_values(geolocation: GeoLocation):
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
    file_header = ','.join(COLUMN_NAMES)
    file_rows = file_header
    for path in locations_path.glob('*'):
        named_location_name = path.parts[location_path_index]
        (description, horizontal_index, vertical_index) = get_named_location_fields(get_named_location,
                                                                                    named_location_name)
        for geolocation in get_geolocations(named_location_name):
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
            x_offset = geolocation.x_offset
            y_offset = geolocation.y_offset
            z_offset = geolocation.z_offset
            pitch = geolocation.alpha
            roll = geolocation.beta
            azimuth = geolocation.gamma
            reference_name = geolocation.offset_name
            reference_description = geolocation.offset_description
            reference_coordinates: Coordinates = get_coordinates(get_geometry(reference_name))
            reference_latitude = reference_coordinates.latitude
            reference_longitude = reference_coordinates.longitude
            reference_elevation = reference_coordinates.elevation
            (x_azimuth, y_azimuth) = find_azimuth_values(geolocation)
            (east_offset, north_offset) = calculate_offsets(x_azimuth, y_azimuth, x_offset, y_offset)

            for reference_geolocation in get_geolocations(reference_name):
                reference_start_date = reference_geolocation.start_date
                reference_end_date = reference_geolocation.end_date
                if reference_start_date is not None:
                    reference_start = date_formatter.to_string(reference_start_date)
                else:
                    reference_start = ''
                if reference_end_date is not None:
                    reference_end = date_formatter.to_string(reference_end_date)
                else:
                    reference_end = ''
                row_data = [f'{horizontal_index}.{vertical_index}',
                            named_location_name,
                            description,
                            start,
                            end,
                            reference_name,
                            reference_description,
                            reference_start,
                            reference_end,
                            str(x_offset),
                            str(y_offset),
                            str(z_offset),
                            str(pitch),
                            str(roll),
                            str(azimuth),
                            reference_latitude,
                            reference_longitude,
                            reference_elevation,
                            str(east_offset),
                            str(north_offset),
                            str(x_azimuth),
                            str(y_azimuth)]
                file_rows += ','.join(row_data) + '\n'
    filename = get_filename(domain=domain,
                            site=site,
                            data_product_id=data_product_id,
                            timestamp=timestamp,
                            file_type='sensor_positions',
                            extension='csv')
    file_path = Path(out_path, site, year, month)
    file_path.mkdir(parents=True, exist_ok=True)
    Path(file_path, filename).write_text(file_rows)
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
