from datetime import datetime
from pathlib import Path
from typing import List, Callable

from publication_files_generator.database_queries.sensor_locations import Location
from publication_files_generator.filename_formatter import format_filename


def column_names() -> List[str]:
    """Reference fields are from the location.nam_locn_id_off column."""
    return [
        'HOR.VER',
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
        'pitch',  # This is calculated from alpha, beta, gamma
        'roll',  # This is calculated from alpha, beta, gamma
        'azimuth',
        'referenceLatitude',
        'referenceLongitude',
        'referenceElevation',
        'eastOffset',
        'northOffset',
        'xAzimuth',
        'yAzimuth'
    ]


def parse_path(path: Path) -> str:
    parts = path.parts
    named_location = parts[4]
    return named_location


def generate_locations_file(in_path: Path,
                            out_path: Path,
                            data_product_id: str,
                            domain: str,
                            site: str,
                            year: str,
                            month: str,
                            timestamp: datetime,
                            get_locations: Callable[[str], List[Location]]) -> str:
    """Generate the sensor locations file and return the filename."""
    rows = ','.join(column_names())
    # TODO: create data rows.
    # Read locations from path, for each location get data and parse out needed columns.
    for path in in_path.glob('*'):
        named_location_name = parse_path(path)
        locations: List[Location] = get_locations(named_location_name)

    filename = format_filename(domain=domain, site=site, data_product_id=data_product_id, timestamp=timestamp,
                               file_type='sensor_positions', extension='csv')
    file_path = Path(out_path, site, year, month, filename)
    file_path.write_text(rows)
    return filename
