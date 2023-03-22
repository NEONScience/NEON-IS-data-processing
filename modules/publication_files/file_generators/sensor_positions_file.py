import csv
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Callable, List

from publication_files.database_queries.geolocation_geometry import get_coordinates
from publication_files.database_queries.named_location import NamedLocation
from publication_files.database_queries.geolocations import GeoLocation
from publication_files.file_metadata import PathElements
from publication_files.filename_format import get_filename

COLUMNS = ['HOR.VER',
           'sensorLocationID', 'sensorLocationDescription',
           'positionStartDateTime', 'positionEndDateTime',
           'referenceLocationID', 'referenceLocationIDDescription',
           'referenceLocationIDStartDateTime', 'referenceLocationIDEndDateTime',
           'xOffset', 'yOffset', 'zOffset',
           'pitch', 'roll', 'azimuth',
           'locationReferenceLatitude', 'locationReferenceLongitude', 'locationReferenceElevation',
           'eastOffset', 'northOffset',
           'xAzimuth', 'yAzimuth']


class SensorPositionsDatabase(NamedTuple):
    get_geolocations: Callable[[str], List[GeoLocation]]
    get_geometry: Callable[[str], str]
    get_named_location: Callable[[str], NamedLocation]


def write_file(out_path: Path, location_path: Path, elements: PathElements, timestamp: datetime,
               database: SensorPositionsDatabase) -> str:
    root = Path(out_path, elements.site, elements.year, elements.month)
    root.mkdir(parents=True, exist_ok=True)
    filename = get_filename(elements, timestamp=timestamp, file_type='sensor_positions', extension='csv')
    with open(Path(root, filename), 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(COLUMNS)
        for path in location_path.glob('*.json'):
            if path.is_file() and path.name.startswith('CFGLOC'):
                named_location_name = path.stem
                named_location = database.get_named_location(named_location_name)
                (horizontal_index, vertical_index) = named_location.get_indices()
                row_hor_ver = f'{horizontal_index}.{vertical_index}'
                row_location_id = named_location.location_id
                row_description = named_location.description
                for geolocation in database.get_geolocations(named_location_name):
                    reference_location_coordinates = get_coordinates(database.get_geometry(geolocation.offset_name))
                    (east_offset, north_offset) = geolocation.get_offsets()
                    (x_azimuth, y_azimuth) = geolocation.get_azimuth_values()
                    (row_position_start_date, row_position_end_date) = geolocation.get_dates()
                    row_x_offset: float = round(geolocation.x_offset, 2)
                    row_y_offset: float = round(geolocation.y_offset, 2)
                    row_z_offset: float = round(geolocation.z_offset, 2)
                    row_pitch: float = round(geolocation.alpha, 2)
                    row_roll: float = round(geolocation.beta, 2)
                    row_azimuth: float = round(geolocation.gamma, 2)
                    row_reference_location_id = (database.get_named_location(geolocation.offset_name)).location_id
                    row_reference_location_description: str = geolocation.offset_description
                    row_reference_location_latitude: float = round(reference_location_coordinates.latitude, 6)
                    row_reference_location_longitude: float = round(reference_location_coordinates.longitude, 6)
                    row_reference_location_elevation: float = round(reference_location_coordinates.elevation, 2)
                    row_x_azimuth: float = round(x_azimuth, 2)
                    row_y_azimuth: float = round(y_azimuth, 2)
                    row_east_offset: float = round(east_offset, 2)
                    row_north_offset: float = round(north_offset, 2)
                    for reference_geolocation in database.get_geolocations(geolocation.offset_name):
                        (row_reference_location_start_date,
                         row_reference_location_end_date) = reference_geolocation.get_dates()
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
