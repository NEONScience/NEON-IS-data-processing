#!/usr/bin/env python3
from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    data_path: Path
    out_path: Path
    pad_dirs: list
    copy_dirs: list
    window_size: int
    year_index: int
    month_index: int
    day_index: int
    location_index: int
    data_type_index: int
    relative_path_index: int
    threshold_dir = 'threshold'
    location_dir = 'location'
    location_file_extension = '.json'
    threshold_filename = 'thresholds.json'
    threshold_file_extension = '.json'
    manifest_filename = 'manifest.txt'
