from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    in_path: Path
    out_path: Path
    context: str
    trim_index: int
    source_id_index: int
    data_type_index: int
