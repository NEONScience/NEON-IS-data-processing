from typing import NamedTuple, List
from datetime import datetime


class Location(NamedTuple):
    start_date: datetime
    end_date: datetime
    transaction_date: datetime
    alpha: float
    beta: float
    gamma: float
    x_offset: float
    y_offset: float
    z_offset: float
    reference_feature: dict


class ReferenceLocation(NamedTuple):
    name: str
    locations: List[Location]
