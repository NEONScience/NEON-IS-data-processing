from functools import partial
from typing import NamedTuple, Callable, List

from data_access.db_connector import DbConnector
from pub_files.database.geolocation_geometry import get_geometry
from pub_files.database.geolocations import get_geolocations
from pub_files.database.named_locations import get_named_location, NamedLocation
from pub_files.geolocation import GeoLocation
from pub_files.geometry import Geometry


class SensorPositionsDatabase(NamedTuple):
    get_geolocations: Callable[[str], List[GeoLocation]]
    get_geometry: Callable[[str], Geometry]
    get_named_location: Callable[[str], NamedLocation]


def get_sensor_positions_database(connector: DbConnector) -> SensorPositionsDatabase:
    return SensorPositionsDatabase(get_geolocations=partial(get_geolocations, connector),
                                   get_geometry=partial(get_geometry, connector),
                                   get_named_location=partial(get_named_location, connector))
