#!/usr/bin/env python3
import structlog
from pathlib import Path

import array_parser.calibration_file_parser as calibration_file_parser
import array_parser.schema_parser as schema_parser
import array_parser.data_file_parser as data_file_parser
from array_parser.array_parser_config import Config
from array_parser.path_parser import PathParser
from array_parser.schema_parser import SchemaData


log = structlog.get_logger()


def parse(config: Config) -> None:
    data_path: Path = config.data_path
    schema_path: Path = config.schema_path
    out_path: Path = config.out_path
    parse_calibration: bool = config.parse_calibration
    test_mode: bool = config.test_mode
    schema_data: SchemaData = schema_parser.parse_schema_file(schema_path)
    parser = PathParser(config)
    for path in data_path.rglob('*'):
        if path.is_file():
            source_type, year, month, day, source_id, data_type = parser.parse(path)
            common_path = Path(out_path, source_type, year, month, day, source_id)
            if data_type == 'data':
                if test_mode:
                    link_data_file(path, Path(common_path, data_type))
                else:
                    data_file_parser.write_restructured_file(path, Path(common_path, data_type), schema_path)
            if parse_calibration and data_type == 'calibration':
                link_calibration_file(path, Path(common_path, data_type), schema_data)


def link_calibration_file(path: Path, out_path, schema_data: SchemaData) -> None:
    stream_id = calibration_file_parser.get_stream_id(path)
    field_name = schema_data.mapping.get(stream_id)
    link_path = Path(out_path, field_name, path.name)
    log.debug(f'calibration link: {link_path}')
    link_path.parent.mkdir(parents=True, exist_ok=True)
    link_path.symlink_to(path)


def link_data_file(path: Path, out_path: Path) -> None:
    link_path = Path(out_path, path.name)
    link_path.parent.mkdir(parents=True, exist_ok=True)
    log.debug(f'data link: {link_path}')
    link_path.symlink_to(path)
