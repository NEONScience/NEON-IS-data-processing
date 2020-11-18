#!/usr/bin/env python3
from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    data_path: Path
    schema_path: Path
    out_path: Path
    parse_calibration: bool
    source_type_index: int
    year_index: int
    month_index: int
    day_index: int
    source_id_index: int
    data_type_index: int
    test_mode: bool
