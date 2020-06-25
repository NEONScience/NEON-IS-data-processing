#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple


class LocationFilePath(object):

    def __init__(self, *, source_type_index: int, year_index: int, month_index: int, location_index: int):
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.location_index = location_index

    def parse(self, path: Path) -> Tuple[str, str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        location: str = parts[self.location_index]
        return source_type, year, month, location
