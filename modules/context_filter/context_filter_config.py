from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    in_path: Path
    out_path: Path
    context: str
    source_type_index: int
    year_index: int
    month_index: int
    day_index: int
    source_id_index: int
    data_type_index: int
