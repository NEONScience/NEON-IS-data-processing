from functools import partial
from typing import NamedTuple, Callable, List

from data_access.db_connector import DbConnector
from pub_files.database.geolocation_geometry import get_geometry
from pub_files.database.geolocations import get_geolocations, GeoLocation
from pub_files.database.named_locations import get_named_location, NamedLocation
from pub_files.geometry import Geometry


class SensorPositionsDatabase(NamedTuple):
    """Class to consolidate the functions to read the data for the sensor positions file from the database."""
    get_geolocations: Callable[[str], List[GeoLocation]]
    get_geometry: Callable[[str], Geometry]
    get_named_location: Callable[[str], NamedLocation]


def get_sensor_positions_database(connector: DbConnector) -> SensorPositionsDatabase:
    """Populate the object with functions hiding the database connection from calling clients."""
    return SensorPositionsDatabase(get_geolocations=partial(get_geolocations, connector),
                                   get_geometry=partial(get_geometry, connector),
                                   get_named_location=partial(get_named_location, connector))
