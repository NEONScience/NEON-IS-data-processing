import csv
from datetime import datetime
from pathlib import Path
from typing import Tuple

import structlog

from pub_files.database.geolocation_geometry import Geometry
from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.sensor_positions.sensor_position import get_position
from pub_files.output_files.sensor_positions.sensor_positions_database import SensorPositionsDatabase

log = structlog.get_logger()


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

    def write(self) -> Path:
        filename = get_filename(self.elements, timestamp=self.timestamp, file_type='sensor_positions', extension='csv')
        file_path = Path(self.out_path, filename)
        with open(file_path, 'w', encoding='UTF8', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(COLUMNS)
            for path in self.location_path.glob('*.json'):
                log.debug(f'path: {path}')
                if path.is_file() and path.name.startswith('CFGLOC'):
                    log.debug(f'file: {path.name}')
                    (row_hor_ver, row_location_id, row_description) = self.get_named_location_data(path.stem)
                    for geolocation in self.database.get_geolocations(path.stem):
                        log.debug(f'geolocation: {geolocation.offset_name}')
                        offset_name = geolocation.offset_name
                        geometry: Geometry = self.database.get_geometry(offset_name)
                        sensor_position = get_position(geolocation)
                        east_offset = sensor_position.east_offset
                        north_offset = sensor_position.north_offset
                        x_azimuth = sensor_position.x_azimuth
                        y_azimuth = sensor_position.y_azimuth
                        row_position_start_date = sensor_position.start_date
                        row_position_end_date = sensor_position.end_date
                        row_x_offset: float = round(geolocation.x_offset, 2)
                        row_y_offset: float = round(geolocation.y_offset, 2)
                        row_z_offset: float = round(geolocation.z_offset, 2)
                        row_pitch: float = round(geolocation.alpha, 2)
                        row_roll: float = round(geolocation.beta, 2)
                        row_azimuth: float = round(geolocation.gamma, 2)
                        row_reference_location_id = (self.database.get_named_location(offset_name)).name
                        row_reference_location_description: str = geolocation.offset_description
                        row_reference_location_latitude: float = round(geometry.latitude, 6)
                        row_reference_location_longitude: float = round(geometry.longitude, 6)
                        row_reference_location_elevation: float = round(geometry.elevation, 2)
                        row_x_azimuth: float = round(x_azimuth, 2) if x_azimuth else ''
                        row_y_azimuth: float = round(y_azimuth, 2) if y_azimuth else ''
                        row_east_offset: float = round(east_offset, 2) if east_offset else ''
                        row_north_offset: float = round(north_offset, 2) if north_offset else ''
                        for reference_geolocation in self.database.get_geolocations(offset_name):
                            reference_position = get_position(reference_geolocation)
                            row_reference_location_start_date = reference_position.start_date
                            row_reference_location_end_date = reference_position.end_date
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
        return file_path

    def get_named_location_data(self, named_location_name: str) -> Tuple[str, str, str]:
        location = self.database.get_named_location(named_location_name)
        (horizontal_index, vertical_index) = location.get_indices()
        hor_ver = f'{horizontal_index}.{vertical_index}'
        return hor_ver, location.name, location.description
