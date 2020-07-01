#!/usr/bin/env python3
from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    location_path: Path
    out_path: Path
    source_type_index: int
    year_index: int
    month_index: int
    location_index: int
