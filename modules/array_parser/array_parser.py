#!/usr/bin/env python3
import structlog
from pathlib import Path

import array_parser.calibration_file_parser as calibration_file_parser
import array_parser.schema_parser as schema_parser
from array_parser.array_parser_config import Config
from array_parser.data_path_parser import DataPathParser

log = structlog.get_logger()


def parse(config: Config):
    parser = DataPathParser(config)
    for path in config.data_path.rglob('*'):
        if path.is_file():
            log.debug(f'data file path: {path}')
            source_type, source_id, year, month, day = parser.parse(path)
            common_path = Path(config.out_path, source_type, year, month, day, source_id)
            link_calibrations(config, common_path, source_id)
