from typing import NamedTuple, Optional
from datetime import datetime

from geojson import FeatureCollection


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
    reference_locations: Optional[FeatureCollection]
