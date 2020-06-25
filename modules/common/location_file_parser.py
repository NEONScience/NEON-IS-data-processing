import geojson
from pathlib import Path
from typing import List


class LocationFileParser(object):
    """Class to parse GEOJson format location files."""

    def __init__(self, path: Path) -> None:
        """
        Parse the location name and the active periods.

        :param path: The file path.
        """
        with open(str(path), 'r') as file:
            geojson_data = geojson.load(file)
            self.properties = geojson_data['properties']
            self.location_name: str = self.properties['name']
            self.active_periods: List[dict] = self.properties['active_periods']

    def get_active_periods(self) -> List[dict]:
        return self.active_periods

    def get_name(self) -> str:
        return self.location_name
