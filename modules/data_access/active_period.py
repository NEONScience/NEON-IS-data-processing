from typing import NamedTuple, Optional


class ActivePeriod(NamedTuple):
    start_date: str
    end_date: Optional[str]
