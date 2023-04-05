from functools import partial

from pub_files.database.queries.geolocation_geometry import get_geometry
from pub_files.database.queries.geolocations import get_geolocations
from pub_files.database.queries.named_locations import get_named_location

from data_access.db_connector import DbConnector
from pub_files.output_files.sensor_positions_file import SensorPositionsDatabase


class SensorPositionsData:

    def __init__(self, connector: DbConnector) -> None:
        self.get_geolocations = partial(get_geolocations, connector)
        self.get_geometry = partial(get_geometry, connector)
        self.get_named_location = partial(get_named_location, connector)

    def get_database(self) -> SensorPositionsDatabase:
        return SensorPositionsDatabase(get_geolocations=self.get_geolocations,
                                       get_geometry=self.get_geometry,
                                       get_named_location=self.get_named_location)
