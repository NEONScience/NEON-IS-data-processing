#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from context_filter.context_filter_config import Config


class PathParser:

    def __init__(self, config: Config) -> None:
        self.source_id_index = config.source_id_index
        self.data_type_index = config.data_type_index

    def parse(self, path: Path) -> Tuple[str, str]:
        parts = path.parts
        source_id: str = parts[self.source_id_index]
        data_type: str = parts[self.data_type_index]
        return source_id, data_type
