from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    source_path: Path
    group: str
    out_path: Path
    source_type_index: int
    year_index: int
    month_index: int
    day_index: int
    location_index: int
    data_type_index: int
