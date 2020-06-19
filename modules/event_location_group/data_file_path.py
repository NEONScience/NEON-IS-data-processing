#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple


class DataFilePath(object):

    def __init__(self, *, source_type_index: int, year_index: int, month_index: int, day_index: int,
                 source_id_index: int):
        self.source_type_index = source_type_index
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.source_id_index = source_id_index

    def parse(self, path: Path) -> Tuple[str, str, str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        source_id: str = parts[self.source_id_index]
        return source_type, year, month, day, source_id
