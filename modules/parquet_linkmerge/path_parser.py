#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from parquet_linkmerge.parquet_linkmerge_config import Config


class PathParser:

    def __init__(self, config: Config) -> None:
        self.source_type_index = config.source_type_index
        self.year_index = config.year_index
        self.month_index = config.month_index
        self.day_index = config.day_index
        self.source_id_index = config.source_id_index

    def parse(self, path: Path) -> Tuple[str, str, str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        year: str = parts[self.year_index]
        month: str = parts[self.month_index]
        day: str = parts[self.day_index]
        source_id: str = parts[self.source_id_index]
        return source_type, year, month, day, source_id
