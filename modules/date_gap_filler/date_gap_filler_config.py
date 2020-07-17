#!/usr/bin/env python3
from pathlib import Path
from datetime import datetime
from typing import NamedTuple, Optional


class DateGapFillerConfig(NamedTuple):
    data_path: Path
    location_path: Path
    empty_file_path: Path
    out_path: Path
    start_date: datetime
    end_date: datetime
    output_directories: list
    data_source_type_index: int
    data_year_index: int
    data_month_index: int
    data_day_index: int
    data_location_index: int
    data_type_index: int
    location_source_type_index: int
    location_year_index: int
    location_month_index: int
    location_day_index: Optional[int]
    location_index: int
    empty_file_type_index: int
    calibration_dir = 'calibration'
    data_dir = 'data'
    flag_dir = 'flags'
    location_dir = 'location'
    uncertainty_coefficient_dir = 'uncertainty_coef'
    uncertainty_data_dir = 'uncertainty_data'
