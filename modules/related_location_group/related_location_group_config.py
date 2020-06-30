#!/usr/bin/env python3
from pathlib import Path
from typing import NamedTuple


class Config(NamedTuple):
    data_path: Path
    out_path: Path
    source_type_index: int
    year_index: int
    month_index: int
    day_index: int
    group_index: int
    location_index: int
    data_type_index: int
