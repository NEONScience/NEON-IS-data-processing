import geojson
from pathlib import Path


def get_active_periods(path: Path):
    """
    Parse the location name and the active periods.

    :param path: The file path.
    :return: The location name and the active periods.
    """
    with open(str(path), 'r') as file:
        geojson_data = geojson.load(file)
        features = geojson_data['features']
        properties = features[0]['properties']
        location_name = properties['name']
        active_periods = properties['active_periods']
    return location_name, active_periods
