#!/usr/bin/env python3
import environs
from pathlib import Path
from structlog import get_logger

import common.log_config as log_config
from path_refactor.mapping import map_reader
from path_refactor.refactor import refactor

log = get_logger()


def main() -> None:
    """
    Read files with path from the list of related paths,
    replace part of path/file names according to mapping,
    link updated path/file names into the output path.
    """
    env = environs.Env()
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    log_config.configure(log_level)

    maps = map_reader()

    refactor(data_path, out_path, relative_path_index, source_id_index, maps)


if __name__ == '__main__':
    main()
