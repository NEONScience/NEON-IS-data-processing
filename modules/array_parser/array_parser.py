#!/usr/bin/env python3
import structlog
from pathlib import Path

import array_parser.calibration_file_parser as calibration_file_parser
import array_parser.schema_parser as schema_parser
import array_parser.data_file_parser as data_file_parser
from array_parser.array_parser_config import Config
from array_parser.path_parser import PathParser
from array_parser.schema_parser import TermMapping


log = structlog.get_logger()


def parse(config: Config) -> None:
    schema_path: Path = config.schema_path
    term_mapping: TermMapping = schema_parser.parse_schema(schema_path)
    parser = PathParser(config)
    for path in config.data_path.rglob('*'):
        if path.is_file():
            source_type, year, month, day, source_id, data_type = parser.parse(path)
            common_path = Path(config.out_path, source_type, year, month, day, source_id)
            if data_type == 'data':
                link_data_file(common_path, path)
            if data_type == 'calibration':
                link_calibration_file(common_path, path, term_mapping)


def link_data_file(common_path: Path, path: Path) -> None:
    link_path = Path(common_path, 'data', path.name)
    link_path.parent.mkdir(parents=True, exist_ok=True)
    log.debug(f'data link: {link_path}')
    link_path.symlink_to(path)


def link_calibration_file(common_path: Path, path: Path, term_mapping: TermMapping) -> None:
    stream_id = calibration_file_parser.get_stream_id(path)
    term_name = term_mapping.mapping.get(stream_id)
    link_path = Path(common_path, 'calibration', term_name, path.name)
    log.debug(f'calibration link: {link_path}')
    link_path.parent.mkdir(parents=True, exist_ok=True)
    link_path.symlink_to(path)
