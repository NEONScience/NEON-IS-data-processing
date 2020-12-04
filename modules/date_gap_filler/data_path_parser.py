#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from date_gap_filler.date_gap_filler_config import DateGapFillerConfig


class DataPathParser:

    def __init__(self, config: DateGapFillerConfig) -> None:
        self.source_type_index = config.data_source_type_index
        self.year_index = config.data_year_index
        self.month_index = config.data_month_index
        self.day_index = config.data_day_index
        self.location_index = config.data_location_index
        self.data_type_index = config.data_type_index

    def parse(self, path: Path) -> Tuple[str, str, str, str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        location: str = parts[self.location_index]
        data_type: str = parts[self.data_type_index]
        return source_type, year, month, day, location, data_type
