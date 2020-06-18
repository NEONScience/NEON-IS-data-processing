import geojson
from pathlib import Path


class LocationFileParser(object):
    """Class to parse GEOJson format location files."""

    def __init__(self, path: Path):
        """
        Parse the location name and the active periods.

        :param path: The file path.
        :return: The location name and the active periods.
        """
        with open(str(path), 'r') as file:
            geojson_data = geojson.load(file)
            features = geojson_data['features']
            self.properties = features[0]['properties']
            self.location_name = self.properties['name']
            self.active_periods = self.properties['active_periods']

    def get_active_periods(self):
        return self.active_periods

    def get_name(self):
        return self.location_name
