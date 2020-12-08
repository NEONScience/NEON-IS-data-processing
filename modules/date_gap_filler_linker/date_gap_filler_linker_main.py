#!/usr/bin/env python3
import structlog
import environs
from pathlib import Path

import common.log_config as log_config
from date_gap_filler_linker.date_gap_filler_linker import DataGapFillerLinker

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    location_index: int = env.int('LOCATION_INDEX')
    empty_file_suffix: str = env.str('EMPTY_FILE_SUFFIX')

    log_config.configure(log_level)

    linker = DataGapFillerLinker(in_path, out_path, relative_path_index, location_index, empty_file_suffix)
    linker.link_files()


if __name__ == '__main__':
    main()
