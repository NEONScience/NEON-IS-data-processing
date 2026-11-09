#!/usr/bin/env python3
from pathlib import Path
from typing import NamedTuple


class Config(NamedTuple):
    # Existing filesystem-mode fields
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
    # Cloud-mode / workflow fields (required when using_filesystem=False)
    source_type: str | None = None
    data_date: str | None = None
    source_id_list: str | None = None
    using_filesystem: bool | None = None
    file_version: str | None = None
    relative_path_index: int | None = None
    common_path: str | None = None
    utils_path: str | None = None
    update_trigger_table: bool | None = None
    data_date: str | None = None
    source_id_list: str | None = None
