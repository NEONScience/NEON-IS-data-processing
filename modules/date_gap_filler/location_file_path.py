#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple, Optional


class LocationFilePath(object):

    def __init__(self, *,
                 source_type_index: int,
                 year_index: int,
                 month_index: int,
                 day_index: Optional[int],
                 location_index: int):
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.location_index = location_index

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
