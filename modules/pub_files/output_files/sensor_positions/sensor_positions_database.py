from typing import NamedTuple, Callable, List

from pub_files.database.queries.named_locations import NamedLocation
from pub_files.geolocation import GeoLocation
from pub_files.geometry import Geometry


class SensorPositionsDatabase(NamedTuple):
    get_geolocations: Callable[[str], List[GeoLocation]]
    get_geometry: Callable[[str], Geometry]
    get_named_location: Callable[[str], NamedLocation]
