from typing import NamedTuple
from pathlib import Path


class Config(NamedTuple):
    data_path: Path
    calibration_path: Path
    out_path: Path
    data_source_type_index: int
    data_source_id_index: int
    data_year_index: int
    data_month_index: int
    data_day_index: int
    calibration_source_type_index: int
    calibration_source_id_index: int
    calibration_stream_index: int
