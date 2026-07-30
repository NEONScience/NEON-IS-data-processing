#!/usr/bin/env python3
from pathlib import Path
from typing import NamedTuple


class Config(NamedTuple):
    data_path: Path
    schema_path: Path
    out_path: Path
    source_type: str
    data_date: str
    source_id_list: str
    parse_calibration: bool
    source_type_index: int
    source_type_out: str
    replace_schema_name: bool
    write_site_file: bool
    year_index: int
    month_index: int
    day_index: int
    source_id_index: int
    data_type_index: int
    test_mode: bool
    using_filesystem: bool
    file_version: str
    common_path: str
    utils_path: str
    update_trigger_table: bool
