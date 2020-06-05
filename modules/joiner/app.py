#!/usr/bin/env python3
import os
from pathlib import Path

from structlog import get_logger
import environs

import common.log_config as log_config
from common.file_crawler import crawl

log = get_logger()


def group(paths: list, out_path: Path, relative_path_index: int):
    """
    Link all files into the output directory.

    :param paths: Paths containing files to process.
    :param out_path: The output path for linking files.
    :param relative_path_index: Trim the input path to this index.
    """
    for path in paths:
        for file_path in crawl(path):
            log.debug(f'file_path: {file_path}')
            parts = file_path.parts
            link_path = Path(out_path, *parts[relative_path_index:])
            log.debug(f'link: {link_path}')
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                link_path.symlink_to(file_path)


def main():
    """Group related paths."""
    env = environs.Env()
    related_paths = env.list('RELATED_PATHS')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log.debug(f'related_paths: {related_paths} out_path: {out_path}')
    paths = []
    for p in related_paths:
        path = os.environ[p]
        paths.append(Path(path))
    group(paths, out_path, relative_path_index)


if __name__ == '__main__':
    main()
