#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple, Optional

from date_gap_filler.date_gap_filler_config import DateGapFillerConfig


class LocationPathParser:

    def __init__(self, config: DateGapFillerConfig) -> None:
        self.source_type_index = config.location_source_type_index
        self.year_index = config.location_year_index
        self.month_index = config.location_month_index
        self.day_index = config.location_day_index
        self.location_index = config.location_index

    def parse(self, path: Path) -> Tuple[str, str, str, Optional[str], str]:
        parts = path.parts
        source_type = parts[self.source_type_index]
        year = parts[self.year_index]
        month = parts[self.month_index]
        day = None
        if self.day_index is not None:
            day = parts[self.day_index]
        location = parts[self.location_index]
        return source_type, year, month, day, location
