from typing import NamedTuple, List, Set

from data_access.types.active_period import ActivePeriod
from data_access.types.property import Property


class NamedLocation(NamedTuple):
    name: str
    type: str
    description: str
    domain: str
    site: str
    schema_names: Set[str]
    context: List[str]
    group: List[str]
    active_periods: List[ActivePeriod]
    properties: List[Property]
