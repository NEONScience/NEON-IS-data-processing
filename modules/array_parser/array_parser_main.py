#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from array_parser.array_parser_config import Config
import array_parser.array_parser as array_parser

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    schema_path: Path = env.path('SCHEMA_PATH')
    out_path: Path = env.path('OUT_PATH')
    parse_calibration = env.bool('PARSE_CALIBRATION')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    test_mode: bool = env.bool("TEST_MODE")
    log.debug(f'data_path: {data_path} schema_path: {schema_path} out_path: {out_path}')
    log_config.configure(log_level)
    config = Config(data_path=data_path,
                    schema_path=schema_path,
                    out_path=out_path,
                    parse_calibration=parse_calibration,
                    source_type_index=source_type_index,
                    year_index=year_index,
                    month_index=month_index,
                    day_index=day_index,
                    source_id_index=source_id_index,
                    data_type_index=data_type_index,
                    test_mode=test_mode)
    array_parser.parse(config)


if __name__ == '__main__':
    main()
