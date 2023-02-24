#!/usr/bin/env python3
from typing import NamedTuple
from pathlib import Path

class Config(NamedTuple):
    in_path: Path
    out_path: Path
    relative_path_index: int
    group_index: int
    group_metadata_index: int
    group_metadata_names: list
    data_type_index: int
    data_type_names: list
