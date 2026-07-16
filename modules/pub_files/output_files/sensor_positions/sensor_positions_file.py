import csv
import json
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
               database: SensorPositionsDatabase,
               position_history_path: Optional[Path] = None) -> Path:
    """Write the sensor positions file to the output path.

    When `position_history_path` is provided and a per-CFGLOC history JSON exists at
    `<position_history_path>/<source_type>/<CFGLOC>/position_history/<CFGLOC>_history.json`,
    the rows for that CFGLOC come from the JSON instead of the DB. This is how
    concH2oSoilSalinity_position_history_loader plugs the full enviroscan CFGLOC
    history into every monthly sensor_positions.csv.
    """

    # Effective-dates columns are emitted only when the loader-driven JSON codepath is in
    # play (i.e. this pipeline is concH2oSoilSalinity today). The DB codepath doesn't yet
    # compute the cfgloc-geo × ref_geolocation intersection needed to fill them, so other
    # DPs keep their pre-change schema until that follow-up lands.
    include_effective_dates = position_history_path is not None

    filename = get_filename(elements, timestamp=timestamp, file_type='sensor_positions', extension='csv')
    file_path = Path(out_path, filename)
    with open(file_path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(get_column_names(include_effective_dates=include_effective_dates))
        file_rows = []
        # Parse location file path for the datum elements. Assume we end at site (**/site/location/*/location_file.json)
        site = location_path.parts[-1]
        for path in location_path.parent.parent.rglob(f'*/{site}/location/*/*.json'):
            if path.is_file() and path.name.startswith('CFGLOC'):
                named_location_name = path.stem

                # Prefer loader-emitted history JSON when available for this CFGLOC.
                history_rows = _read_position_history_rows(position_history_path, named_location_name, database)
                if history_rows is not None:
                    for row in history_rows:
                        if row not in file_rows:
                            file_rows.append(row)
                    continue

                location = database.get_named_location(named_location_name)
                (row_hor_ver, row_location_id, row_description) = get_named_location_data(database, named_location_name)
                geolocations = database.get_geolocations(named_location_name)

                for geolocation in geolocations:
                    # Use the specified processing method
                    if sensor_specific_processors.is_tchain_sensor(location):
                        rows = sensor_specific_processors.create_tchain_rows(
                            database, location, geolocation, row_hor_ver,
                            row_location_id, row_description,
                            _create_base_row_data, _add_reference_position_data,
                            include_effective_dates=include_effective_dates)
                    else:
                        rows = _create_standard_rows(database, geolocation, row_hor_ver,
                                                   row_location_id, row_description,
                                                   include_effective_dates=include_effective_dates)

                    # Add rows, preventing duplicates
                    for row in rows:
                        if row not in file_rows:
                            file_rows.append(row)

        writer.writerows(file_rows)
    return file_path


def _read_position_history_rows(position_history_path: Optional[Path],
                                named_location_name: str,
                                database: SensorPositionsDatabase) -> Optional[List[List]]:
    """Load rows from the loader-emitted history JSON, or None if unavailable.

    Returns a list of CSV rows in the same column order as `get_column_names()`.
    Any CFGLOC without a matching JSON falls back to the DB-driven path.
    """
    if position_history_path is None:
        return None
    # Loader writes to /<source_type>/<CFGLOC>/position_history/<CFGLOC>_history.json.
    # Only enviroscan is emitted today; expand the source_type list as more loaders come online.
    candidates = [position_history_path / source_type / named_location_name / 'position_history'
                                        / f'{named_location_name}_history.json'
                  for source_type in ('enviroscan',)]
    json_path = next((p for p in candidates if p.is_file()), None)
    if json_path is None:
        return None

    with open(json_path, 'r') as fp:
        payload = json.load(fp)
    location = database.get_named_location(named_location_name)
    row_description = location.description
    row_location_id = location.name

    # Reference-location azimuths (for east/north offset math) live in the DB; cache per ref name.
    ref_geolocation_cache: Dict[str, List] = {}

    csv_rows: List[List] = []
    for entry in payload.get('rows', []):
        ref_name = entry.get('reference_location_id')
        if ref_name not in ref_geolocation_cache:
            ref_geolocation_cache[ref_name] = database.get_geolocations(ref_name) if ref_name else []
        ref_geolocation = _match_reference_geolocation(ref_geolocation_cache[ref_name], entry)
        if ref_geolocation is not None:
            reference_position = get_position(ref_geolocation, entry['x_offset'], entry['y_offset'])
            east_offset = reference_position.east_offset
            north_offset = reference_position.north_offset
            x_azimuth = reference_position.x_azimuth
            y_azimuth = reference_position.y_azimuth
            ref_description = database.get_named_location(ref_name).description if ref_name else ''
        else:
            east_offset = north_offset = x_azimuth = y_azimuth = ''
            ref_description = database.get_named_location(ref_name).description if ref_name else ''

        csv_rows.append([
            f"{entry['hor']}.{entry['ver']}",
            row_location_id,
            row_description,
            entry.get('effective_start_date', ''),
            entry.get('effective_end_date', ''),
            entry.get('position_start_date', ''),
            entry.get('position_end_date', ''),
            ref_name or '',
            ref_description,
            entry.get('reference_location_start_date', '') or '',
            entry.get('reference_location_end_date', '') or '',
            round(entry['x_offset'], 2) if entry.get('x_offset') is not None else '',
            round(entry['y_offset'], 2) if entry.get('y_offset') is not None else '',
            round(entry['z_offset'], 2) if entry.get('z_offset') is not None else '',
            round(entry['pitch'], 2) if entry.get('pitch') is not None else '',
            round(entry['roll'], 2) if entry.get('roll') is not None else '',
            round(entry['azimuth'], 2) if entry.get('azimuth') is not None else '',
            round(entry['reference_location_latitude'], 6) if entry.get('reference_location_latitude') is not None else '',
            round(entry['reference_location_longitude'], 6) if entry.get('reference_location_longitude') is not None else '',
            round(entry['reference_location_elevation'], 2) if entry.get('reference_location_elevation') is not None else '',
            round(east_offset, 2) if isinstance(east_offset, (int, float)) or hasattr(east_offset, 'quantize') else east_offset,
            round(north_offset, 2) if isinstance(north_offset, (int, float)) or hasattr(north_offset, 'quantize') else north_offset,
            round(x_azimuth, 2) if isinstance(x_azimuth, (int, float)) else x_azimuth,
            round(y_azimuth, 2) if isinstance(y_azimuth, (int, float)) else y_azimuth,
        ])
    return csv_rows


def _match_reference_geolocation(ref_geolocations: List, entry: Dict):
    """Pick the reference geolocation whose validity window overlaps the JSON entry's position window."""
    if not ref_geolocations:
        return None
    entry_start = _strip_tz(_parse_iso(entry.get('position_start_date')))
    entry_end = _strip_tz(_parse_iso(entry.get('position_end_date')))
    for ref in ref_geolocations:
        ref_start = _strip_tz(ref.start_date)
        ref_end = _strip_tz(ref.end_date)
        if entry_end is not None and ref_start is not None and entry_end <= ref_start:
            continue
        if entry_start is not None and ref_end is not None and entry_start >= ref_end:
            continue
        return ref
    # Fall back to the first available so downstream still gets azimuth-based east/north math.
    return ref_geolocations[0]


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return date_formatter.to_datetime(value)
    except (ValueError, AttributeError):
        return None


def _strip_tz(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=None) if dt.tzinfo is not None else dt


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
        
        # Determine if this reference position is applicable based on the time.
        # If reference location and geolocation don't overlap, skip
        if (reference_geolocation.end_date is None) and (geolocation.end_date is not None):
            if geolocation.end_date <= reference_geolocation.start_date:
                continue
        elif (geolocation.end_date is None) and (reference_geolocation.end_date is not None):
            if geolocation.start_date >= reference_geolocation.end_date:
                continue
        elif (geolocation.end_date is not None) and (reference_geolocation.end_date is not None):
            if geolocation.end_date <= reference_geolocation.start_date:
                continue
            elif geolocation.start_date >= reference_geolocation.end_date:
                continue
        
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


def _create_standard_rows(database: SensorPositionsDatabase, geolocation,
                        row_hor_ver: str, row_location_id: str, row_description: str,
                        include_effective_dates: bool = False) -> List[List]:
    """Create a standard sensor rows.

    When `include_effective_dates` is True, the row leaves two blank cells for
    `effectiveStartDateTime` / `effectiveEndDateTime` so it aligns with the header the
    JSON codepath emits. The DB codepath doesn't yet compute the intersection needed to
    populate them; when it does, this parameter's default can flip.
    """
    base_data = _create_base_row_data(database, geolocation, row_hor_ver, row_location_id, row_description)
    complete_rows = _add_reference_position_data(database, base_data, geolocation, geolocation.offset_name)

    # Convert to list format (assuming we take the first reference location for standard sensors)
    rows=[]
    if complete_rows:
        for row_data in complete_rows:
            leading = [
                row_data['row_hor_ver'],
                row_data['row_location_id'],
                row_data['row_description'],
            ]
            if include_effective_dates:
                leading.extend(['', ''])
            row = leading + [
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
            rows.append(row)
    return rows


def get_column_names(include_effective_dates: bool = False) -> List[str]:
    """Return the CSV header for sensor_positions.csv.

    When `include_effective_dates` is True, two extra columns
    (`effectiveStartDateTime` / `effectiveEndDateTime`) are inserted between
    `sensorLocationDescription` and `positionStartDateTime`. These carry the
    non-overlapping timeline that combines position and reference-location date
    ranges; only the JSON codepath (concH2oSoilSalinity loader) populates them
    today. The DB codepath keeps the pre-change schema until it's separately
    updated to compute the cfgloc-geo × ref_geolocation intersection.
    """
    columns = ['HOR.VER',
               'sensorLocationID',
               'sensorLocationDescription']
    if include_effective_dates:
        columns.extend(['effectiveStartDateTime', 'effectiveEndDateTime'])
    columns.extend(['positionStartDateTime',
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
                    'yAzimuth'])
    return columns


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
