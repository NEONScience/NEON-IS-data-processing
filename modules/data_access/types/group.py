from typing import NamedTuple, List, Set

from data_access.types.active_period import ActivePeriod
from data_access.types.property import Property


class Group(NamedTuple):
    name: str
    type: str
    group: str
    active_periods: List[ActivePeriod]
    properties: List[Property]
