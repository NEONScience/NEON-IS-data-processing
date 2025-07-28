import csv
from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Dict, Optional

import common.date_formatter as date_formatter
from pub_files.database.geolocation_geometry import Geometry
from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.sensor_positions.sensor_position import get_position
from pub_files.output_files.sensor_positions.sensor_positions_database import SensorPositionsDatabase
from pub_files.output_files.sensor_positions import sensor_specific_processors


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
                location = database.get_named_location(named_location_name)
                (row_hor_ver, row_location_id, row_description) = get_named_location_data(database, named_location_name)
                geolocations = database.get_geolocations(named_location_name)
                
                for geolocation in geolocations:
                    # Use the specified processing method
                    if sensor_specific_processors.is_tchain_sensor(location):
                        rows = sensor_specific_processors.create_tchain_rows(
                            database, location, geolocation, row_hor_ver, 
                            row_location_id, row_description,
                            _create_base_row_data, _add_reference_position_data)
                    else:
                        rows = [_create_standard_row(database, geolocation, row_hor_ver, 
                                                   row_location_id, row_description)]
                    
                    # Add rows, preventing duplicates
                    for row in rows:
                        if row not in file_rows:
                            file_rows.append(row)
        
        writer.writerows(file_rows)
    return file_path


def _create_base_row_data(database: SensorPositionsDatabase, geolocation, 
                         row_hor_ver: str, row_location_id: str, row_description: str) -> Dict:
    """Create the common row data used by both standard and specific sensors."""
    offset_name = geolocation.offset_name
    offset_geometry: Geometry = database.get_geometry(offset_name)
    
    # Basic geolocation data
    base_data = {
        'row_hor_ver': row_hor_ver,
        'row_location_id': row_location_id,
        'row_description': row_description,
        'row_position_start_date': format_date(geolocation.start_date),
        'row_position_end_date': format_date(geolocation.end_date),
        'row_x_offset': round(geolocation.x_offset, 2),
        'row_y_offset': round(geolocation.y_offset, 2),
        'row_z_offset': round(geolocation.z_offset, 2),
        'row_pitch': round(geolocation.alpha, 2),
        'row_roll': round(geolocation.beta, 2),
        'row_azimuth': round(geolocation.gamma, 2),
        'row_reference_location_id': (database.get_named_location(offset_name)).name,
        'row_reference_location_description': geolocation.offset_description,
        'row_reference_location_latitude': round(offset_geometry.latitude, 6) if offset_geometry.latitude is not None else None,
        'row_reference_location_longitude': round(offset_geometry.longitude, 6) if offset_geometry.longitude is not None else None,
        'row_reference_location_elevation': round(offset_geometry.elevation, 2) if offset_geometry.elevation is not None else None,
    }
    
    return base_data


def _add_reference_position_data(database: SensorPositionsDatabase, base_data: Dict, 
                                geolocation, offset_name: str) -> List[Dict]:
    """Add reference position data to base row data and return list of complete row data."""
    complete_rows = []
    
    for reference_geolocation in database.get_geolocations(offset_name):
        reference_position = get_position(reference_geolocation, geolocation.x_offset, geolocation.y_offset)
        
        complete_row_data = base_data.copy()
        complete_row_data.update({
            'row_x_azimuth': round(reference_position.x_azimuth, 2) if reference_position.x_azimuth is not None else '',
            'row_y_azimuth': round(reference_position.y_azimuth, 2) if reference_position.y_azimuth is not None else '',
            'row_east_offset': round(reference_position.east_offset, 2) if reference_position.east_offset is not None else '',
            'row_north_offset': round(reference_position.north_offset, 2) if reference_position.north_offset is not None else '',
            'row_reference_location_start_date': format_date(reference_geolocation.start_date),
            'row_reference_location_end_date': format_date(reference_geolocation.end_date),
        })
        complete_rows.append(complete_row_data)
    
    return complete_rows


def _create_standard_row(database: SensorPositionsDatabase, geolocation, 
                        row_hor_ver: str, row_location_id: str, row_description: str) -> List:
    """Create a standard sensor row."""
    base_data = _create_base_row_data(database, geolocation, row_hor_ver, row_location_id, row_description)
    complete_rows = _add_reference_position_data(database, base_data, geolocation, geolocation.offset_name)
    
    # Convert to list format (assuming we take the first reference location for standard sensors)
    if complete_rows:
        row_data = complete_rows[0]
        return [
            row_data['row_hor_ver'],
            row_data['row_location_id'],
            row_data['row_description'],
            row_data['row_position_start_date'],
            row_data['row_position_end_date'],
            row_data['row_reference_location_id'],
            row_data['row_reference_location_description'],
            row_data['row_reference_location_start_date'],
            row_data['row_reference_location_end_date'],
            row_data['row_x_offset'],
            row_data['row_y_offset'],
            row_data['row_z_offset'],
            row_data['row_pitch'],
            row_data['row_roll'],
            row_data['row_azimuth'],
            row_data['row_reference_location_latitude'],
            row_data['row_reference_location_longitude'],
            row_data['row_reference_location_elevation'],
            row_data['row_east_offset'],
            row_data['row_north_offset'],
            row_data['row_x_azimuth'],
            row_data['row_y_azimuth']
        ]
    return []


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
