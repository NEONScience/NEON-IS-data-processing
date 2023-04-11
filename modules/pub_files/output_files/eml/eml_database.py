from typing import NamedTuple, Callable, List, Iterator

from data_access.types.threshold import Threshold
from pub_files.database.queries.named_locations import NamedLocation
from pub_files.database.queries.units import EmlUnitType
from pub_files.database.queries.value_list import Value
from pub_files.geometry import Geometry


class EmlDatabase(NamedTuple):
    get_named_location: Callable[[str], NamedLocation]
    get_geometry: Callable[[str], Geometry]
    get_spatial_unit: Callable[[int], str]
    get_value_list: Callable[[str], List[Value]]
    get_thresholds: Callable[[str], Iterator[Threshold]]
    get_unit_eml_type: Callable[[str], EmlUnitType]
