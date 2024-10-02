#!/usr/bin/env python3
import environs
from pathlib import Path
from structlog import get_logger

from raw_data_parser.raw_data_parser import parse_raw
import common.log_config as log_config

log = get_logger()


def main() -> None:
    env = environs.Env()
    source_type: str = env.str('SOURCE_TYPE')
    parse_field: str = env.str('PARSE_FIELD')
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)

    log.debug(f'input path is {data_path}.')
    parse_raw(source_type, parse_field, data_path, out_path, relative_path_index)


if __name__ == "__main__":
    main()
