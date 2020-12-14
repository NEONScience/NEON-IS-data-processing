#!/usr/bin/env python3
import os
from pathlib import Path

from structlog import get_logger
import environs

import common.log_config as log_config
from joiner.joiner import join_files

log = get_logger()


def main() -> None:
    """Read files from the list of related paths and link them into the output path."""
    env = environs.Env()
    related_paths: list = env.list('RELATED_PATHS')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log.debug(f'related_paths: {related_paths} out_path: {out_path}')
    paths = []
    for p in related_paths:
        path = os.environ[p]
        paths.append(Path(path))
    join_files(related_paths=paths, out_path=out_path, relative_path_index=relative_path_index)


if __name__ == '__main__':
    main()
