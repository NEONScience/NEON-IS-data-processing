from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    data_path: Path
    location_path: Path
    out_path: Path
    source_type_index: int
    year_index: int
    month_index: int
    day_index: int
