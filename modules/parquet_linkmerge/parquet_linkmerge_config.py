#!/usr/bin/env python3
from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    in_path: Path
    out_path: Path
    duplication_threshold: float
    source_type_index: int
    year_index: int
    month_index: int
    day_index: int
    source_id_index: int
