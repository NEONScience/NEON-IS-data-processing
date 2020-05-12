#!/usr/bin/env python3
import os
import pathlib

from structlog import get_logger
import environs

import lib.log_config as log_config
from lib.file_linker import link
from lib.file_crawler import crawl

log = get_logger()


def group(paths, out_path, relative_path_index):
    """
    Link all files into the output directory.

    :param paths: Environment variables whose values are full directory paths.
    :type paths: list
    :param out_path: The output path for writing results.
    :type out_path: str
    :param relative_path_index: Trim the input path to this index.
    :type relative_path_index: int
    """
    for path in paths:
        for file_path in crawl(path):
            log.debug(f'file_path: {file_path}')
            parts = pathlib.Path(file_path).parts
            target = os.path.join(out_path, *parts[relative_path_index:])
            log.debug(f'target: {target}')
            link(file_path, target)


def main():
    """Group related paths."""
    env = environs.Env()
    related_paths = env.list('RELATED_PATHS')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log.debug(f'related_paths: {related_paths} out_path: {out_path}')
    paths = []
    for p in related_paths:
        path = os.environ[p]
        paths.append(path)
    group(paths, out_path, relative_path_index)


if __name__ == '__main__':
    main()
