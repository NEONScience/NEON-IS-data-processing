#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from data_calibration_linker.data_calibration_config import Config


class DataPathParser:

    def __init__(self, config: Config) -> None:
        self.source_type_index = config.data_source_type_index
        self.source_id_index = config.data_source_id_index
        self.year_index = config.data_year_index
        self.month_index = config.data_month_index
        self.day_index = config.data_day_index

    def parse(self, path: Path) -> Tuple[str, str, str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        source_id: str = parts[self.source_id_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        return source_type, source_id, year, month, day
