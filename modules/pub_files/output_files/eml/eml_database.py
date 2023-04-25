from functools import partial
from typing import NamedTuple, Callable, List, Iterator

from data_access.db_connector import DbConnector
from data_access.get_thresholds import get_thresholds
from data_access.types.threshold import Threshold
from pub_files.database.geolocation_geometry import get_geometry
from pub_files.database.named_locations import NamedLocation
from pub_files.database.named_locations import get_named_location
from pub_files.database.spatial_units import get_spatial_unit
from pub_files.database.units import EmlUnitType
from pub_files.database.units import get_unit_eml_type
from pub_files.database.value_list import Value
from pub_files.database.value_list import get_value_list
from pub_files.geometry import Geometry


class EmlDatabase(NamedTuple):
    get_named_location: Callable[[str], NamedLocation]
    get_geometry: Callable[[str], Geometry]
    get_spatial_unit: Callable[[int], str]
    get_value_list: Callable[[str], List[Value]]
    get_thresholds: Callable[[str], Iterator[Threshold]]
    get_unit_eml_type: Callable[[str], EmlUnitType]


def get_eml_database(connector: DbConnector) -> EmlDatabase:
    return EmlDatabase(get_named_location=partial(get_named_location, connector),
                       get_geometry=partial(get_geometry, connector),
                       get_spatial_unit=partial(get_spatial_unit, connector),
                       get_value_list=partial(get_value_list, connector),
                       get_thresholds=partial(get_thresholds, connector),
                       get_unit_eml_type=partial(get_unit_eml_type, connector))
