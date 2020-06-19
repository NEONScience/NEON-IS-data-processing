#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple


class DataFilePath(object):

    def __init__(self, *, source_type_index: int, year_index: int, month_index: int, day_index: int,
                 location_index: int, data_type_index: int):
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.location_index = location_index
        self.data_type_index = data_type_index

    def parse(self, path: Path) -> Tuple[str, str, str, str, str, str, Tuple[str]]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        location: str = parts[self.location_index]
        data_type: str = parts[self.data_type_index]
        remainder: Tuple[str] = parts[self.data_type_index + 1:]
        return source_type, year, month, day, location, data_type, remainder
