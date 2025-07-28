import csv
from datetime import datetime
from pathlib import Path
from typing import Tuple, List

import common.date_formatter as date_formatter
from pub_files.database.geolocation_geometry import Geometry
from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.sensor_positions.sensor_position import get_position
from pub_files.output_files.sensor_positions.sensor_positions_database import SensorPositionsDatabase

# Temporary - for tchain
from pub_files.output_files.sensor_positions.sensor_position import get_property


def write_file(out_path: Path, location_path: Path, elements: PathElements, timestamp: datetime,
               database: SensorPositionsDatabase) -> Path:
    """Write the sensor positions file to the output path."""
    filename = get_filename(elements, timestamp=timestamp, file_type='sensor_positions', extension='csv')
    file_path = Path(out_path, filename)
    with open(file_path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(get_column_names())
        file_rows = []
        # Parse location file path for the datum elements. Assume we end at site (**/site/location/*/location_file.json)
        site = location_path.parts[-1]
        for path in location_path.parent.parent.rglob(f'*/{site}/location/*/*.json'):
            if path.is_file() and path.name.startswith('CFGLOC'):
                named_location_name = path.stem
                (row_hor_ver, row_location_id, row_description) = get_named_location_data(database, named_location_name)
                geolocations = database.get_geolocations(named_location_name)
                for geolocation in geolocations:
                    # offset location
                    offset_name = geolocation.offset_name
                    offset_geometry: Geometry = database.get_geometry(offset_name)
                    # set row values with formatting
                    row_position_start_date: str = format_date(geolocation.start_date)
                    row_position_end_date: str = format_date(geolocation.end_date)
                    row_x_offset: float = round(geolocation.x_offset, 2)
                    row_y_offset: float = round(geolocation.y_offset, 2)
                    row_z_offset: float = round(geolocation.z_offset, 2)
                    
                    
                    # Temp stuff for tchain
                    location = database.get_named_location(named_location_name)
                    p1=get_property(location.properties,'ThermistorDepth501')
                    p2=get_property(location.properties,'ThermistorDepth502')
                    p3=get_property(location.properties,'ThermistorDepth503')
                    p4=get_property(location.properties,'ThermistorDepth504')
                    p5=get_property(location.properties,'ThermistorDepth505')
                    p6=get_property(location.properties,'ThermistorDepth506')
                    p7=get_property(location.properties,'ThermistorDepth507')
                    p8=get_property(location.properties,'ThermistorDepth508')
                    p9=get_property(location.properties,'ThermistorDepth509')
                    p10=get_property(location.properties,'ThermistorDepth510')
                    p11=get_property(location.properties,'ThermistorDepth511')
                    print(f'ThermistorDepth501={p1}')
                    print(f'ThermistorDepth502={p2}')
                    print(f'ThermistorDepth503={p3}')
                    print(f'ThermistorDepth504={p4}')
                    print(f'ThermistorDepth505={p5}')
                    print(f'ThermistorDepth506={p6}')
                    print(f'ThermistorDepth507={p7}')
                    print(f'ThermistorDepth508={p8}')
                    print(f'ThermistorDepth509={p9}')
                    print(f'ThermistorDepth510={p10}')
                    print(f'ThermistorDepth511={p11}')
                    
                    
                    row_pitch: float = round(geolocation.alpha, 2)
                    row_roll: float = round(geolocation.beta, 2)
                    row_azimuth: float = round(geolocation.gamma, 2)
                    row_reference_location_id: str = (database.get_named_location(offset_name)).name
                    row_reference_location_description: str = geolocation.offset_description
                    row_reference_location_latitude = None
                    row_reference_location_longitude = None
                    row_reference_location_elevation = None
                    if offset_geometry.latitude is not None:
                        row_reference_location_latitude: float = round(offset_geometry.latitude, 6)
                    if offset_geometry.longitude is not None:
                        row_reference_location_longitude: float = round(offset_geometry.longitude, 6)
                    if offset_geometry.elevation is not None:
                        row_reference_location_elevation: float = round(offset_geometry.elevation, 2)
                    # reference location
                    for reference_geolocation in database.get_geolocations(offset_name):
                        reference_position = get_position(reference_geolocation, geolocation.x_offset,
                                                          geolocation.y_offset)
                        x_azimuth = reference_position.x_azimuth
                        y_azimuth = reference_position.y_azimuth
                        east_offset = reference_position.east_offset
                        north_offset = reference_position.north_offset
                        row_x_azimuth: float = round(x_azimuth, 2) if x_azimuth is not None else ''
                        row_y_azimuth: float = round(y_azimuth, 2) if y_azimuth is not None else ''
                        row_east_offset: float = round(east_offset, 2) if east_offset is not None else ''
                        row_north_offset: float = round(north_offset, 2) if north_offset is not None else ''
                        row_reference_location_start_date = format_date(reference_geolocation.start_date)
                        row_reference_location_end_date = format_date(reference_geolocation.end_date)
                        # create row
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
                        if row not in file_rows:  # prevent duplicates
                            file_rows.append(row)
        writer.writerows(file_rows)
    return file_path


def get_column_names() -> List[str]:
    return ['HOR.VER',
            'sensorLocationID',
            'sensorLocationDescription',
            'positionStartDateTime',
            'positionEndDateTime',
            'referenceLocationID',
            'referenceLocationIDDescription',
            'referenceLocationIDStartDateTime',
            'referenceLocationIDEndDateTime',
            'xOffset',
            'yOffset',
            'zOffset',
            'pitch',
            'roll',
            'azimuth',
            'locationReferenceLatitude',
            'locationReferenceLongitude',
            'locationReferenceElevation',
            'eastOffset',
            'northOffset',
            'xAzimuth',
            'yAzimuth']


def format_date(date: datetime) -> str:
    """Return the given date in a string formatted for the sensor positions file."""
    if date is not None:
        return date_formatter.to_string(date)
    return ''


def get_named_location_data(database: SensorPositionsDatabase, named_location_name: str) -> Tuple[str, str, str]:
    """Get the named location data for the given named location name."""
    location = database.get_named_location(named_location_name)
    (horizontal_index, vertical_index) = location.get_indices()
    hor_ver = f'{horizontal_index}.{vertical_index}'
    return hor_ver, location.name, location.description
