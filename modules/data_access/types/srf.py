from typing import NamedTuple, Optional
from datetime import datetime


class Srf(NamedTuple):
    group_name: str
    id: int
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    measurement_stream_name: str
    srf_term_name: str
    srf: int
    user_comment: str
    create_date: Optional[datetime]
    last_update_date: Optional[datetime]
