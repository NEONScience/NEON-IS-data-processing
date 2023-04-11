import csv
from datetime import datetime
from pathlib import Path
from typing import Tuple

from pub_files.database.queries.geolocation_geometry import Geometry
from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.sensor_positions.sensor_positions_database import SensorPositionsDatabase

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


class SensorPositionsFile:

    def __init__(self, out_path: Path, location_path: Path, elements: PathElements, timestamp: datetime,
                 database: SensorPositionsDatabase):
        self.out_path = out_path
        self.location_path = location_path
        self.elements = elements
        self.timestamp = timestamp
        self.database = database

    def write(self) -> str:
        filename = get_filename(self.elements, timestamp=self.timestamp, file_type='sensor_positions', extension='csv')
        with open(Path(self.out_path, filename), 'w', encoding='UTF8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(COLUMNS)
            for path in self.location_path.glob('*.json'):
                if path.is_file() and path.name.startswith('CFGLOC'):
                    (row_hor_ver, row_location_id, row_description) = self._get_named_location_data(path.stem)
                    for geolocation in self.database.get_geolocations(path.stem):
                        offset_name = geolocation.offset_name
                        geometry: Geometry = self.database.get_geometry(offset_name)
                        (east_offset, north_offset) = geolocation.get_offsets()
                        (x_azimuth, y_azimuth) = geolocation.get_azimuth_values()
                        (row_position_start_date, row_position_end_date) = geolocation.get_dates()
                        row_x_offset: float = round(geolocation.x_offset, 2)
                        row_y_offset: float = round(geolocation.y_offset, 2)
                        row_z_offset: float = round(geolocation.z_offset, 2)
                        row_pitch: float = round(geolocation.alpha, 2)
                        row_roll: float = round(geolocation.beta, 2)
                        row_azimuth: float = round(geolocation.gamma, 2)
                        row_reference_location_id = (self.database.get_named_location(offset_name)).location_id
                        row_reference_location_description: str = geolocation.offset_description
                        row_reference_location_latitude: float = round(geometry.latitude, 6)
                        row_reference_location_longitude: float = round(geometry.longitude, 6)
                        row_reference_location_elevation: float = round(geometry.elevation, 2)
                        row_x_azimuth: float = round(x_azimuth, 2)
                        row_y_azimuth: float = round(y_azimuth, 2)
                        row_east_offset: float = round(east_offset, 2)
                        row_north_offset: float = round(north_offset, 2)
                        for reference_geolocation in self.database.get_geolocations(offset_name):
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

    def _get_named_location_data(self, named_location_name: str) -> Tuple[str, int, str]:
        location = self.database.get_named_location(named_location_name)
        (horizontal_index, vertical_index) = location.get_indices()
        hor_ver = f'{horizontal_index}.{vertical_index}'
        return hor_ver, location.location_id, location.description
