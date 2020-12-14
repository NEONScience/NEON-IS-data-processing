#!/usr/bin/env python3
from pathlib import Path
from typing import Tuple

from data_calibration_group.data_calibration_group_config import Config


class CalibrationPathParser:

    def __init__(self, config: Config) -> None:
        self.source_type_index = config.calibration_source_type_index
        self.source_id_index = config.calibration_source_id_index
        self.stream_index = config.calibration_stream_index

    def parse(self, path: Path) -> Tuple[str, str, str]:
        parts = path.parts
        source_type: str = parts[self.source_type_index]
        source_id: str = parts[self.source_id_index]
        stream: str = parts[self.stream_index]
        return source_type, source_id, stream
