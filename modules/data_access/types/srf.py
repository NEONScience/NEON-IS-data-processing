from typing import NamedTuple, List, Optional


class Srf(NamedTuple):
    group_name: str
    id: int
    start_date: datetime
    end_date: datetime
    measurement_stream_name: str
    srf_term_name: str
    srf: int
    user_comment: str
    create_date: datetime
    last_update_date: datetime
