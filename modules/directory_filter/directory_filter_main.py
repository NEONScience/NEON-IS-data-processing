#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from directory_filter.directory_filter import filter_directory


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    filter_dirs: list = env.list('FILTER_DIR')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} filter_dirs: {filter_dirs} out_dir: {out_path}')
    filter_directory(in_path, out_path, filter_dirs, relative_path_index)


if __name__ == '__main__':
    main()
