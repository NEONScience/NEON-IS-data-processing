#!/usr/bin/env python3
from pathlib import Path
from typing import NamedTuple


class Config(NamedTuple):
    # Existing filesystem-mode fields (kept first for backwards compatibility)
    data_path: Path
    schema_path: Path
    out_path: Path
    parse_calibration: bool
    source_type_index: int
    source_type_out: str | None
    replace_schema_name: bool
    write_site_file: bool
    year_index: int
    month_index: int
    day_index: int
    source_id_index: int
    data_type_index: int
    test_mode: bool

    # Cloud-mode / workflow fields (optional; required only when using_filesystem=False)
    source_type: str | None = None
    data_date: str | None = None
    source_id_list: str | None = None
    using_filesystem: bool = True
    file_version: str = "v2"
    common_path: str | None = None
    utils_path: str | None = None
    update_trigger_table: bool = False
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
