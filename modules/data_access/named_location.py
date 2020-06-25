from typing import NamedTuple, List

from data_access.active_period import ActivePeriod
from data_access.property import Property


class NamedLocation(NamedTuple):
    name: str
    type: str
    description: str
    site: str
    context: List[str]
    active_periods: List[ActivePeriod]
    properties: List[Property]
