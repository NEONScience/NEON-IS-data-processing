from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    group_assignment_path: Path
    location_focus_path: Path
    group_focus_path: Path
    group: str
    out_path: Path
    err_path: Path
    group_assignment_year_index: int
    group_assignment_month_index: int
    group_assignment_day_index: int
    group_assignment_member_index: int
    group_assignment_data_type_index: int
    location_focus_source_type_index: int
    location_focus_year_index: int
    location_focus_month_index: int
    location_focus_day_index: int
    location_focus_location_index: int
    group_focus_year_index: int
    group_focus_month_index: int
    group_focus_day_index: int
    group_focus_group_index: int
