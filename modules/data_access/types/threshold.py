from typing import NamedTuple, List, Optional


class Threshold(NamedTuple):
    threshold_name: str
    term_name: str
    location_name: str
    context: List[str]
    start_date: str
    end_date: Optional[str]
    is_date_constrained: bool
    start_day_of_year: int
    end_day_of_year: int
    number_value: float
    string_value: str
