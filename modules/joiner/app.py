#!/usr/bin/env python3
import os

from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = get_logger()


def group(paths, out_path):
    """
    Link all files into the output directory.
    :param paths: Comma separated list of environment variable names whose values are full directory paths.
    :param out_path: The output path for writing results.
    """
    if ',' in paths:
        paths = paths.split(',')
    log.debug(f'paths: {paths}')
    for p in paths:
        log.debug(f'path: {p}')
        path = os.environ[p]
        for file_path in file_crawler.crawl(path):
            target = target_path.get_path(file_path, out_path)
            log.debug(f'target: {target}')
            file_linker.link(file_path, target)


def main():
    """
    Group related paths without modifying the paths.
    """
    env = environs.Env()
    related_paths = env('RELATED_PATHS')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'related_paths: {related_paths} out_path: {out_path}')
    group(related_paths, out_path)


if __name__ == '__main__':
    main()
