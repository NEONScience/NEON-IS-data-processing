#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger
import environs

import common.log_config as log_config
from common.file_crawler import crawl

log = get_logger()


def group(path: Path, out_path: Path, relative_path_index: int):
    """
    Link files into the output directory.

    :param path: File or directory paths.
    :param out_path: The output path for writing results.
    :param relative_path_index: Trim path components before this index.
    """
    for file_path in crawl(path):
        parts = file_path.parts
        link_path = Path(out_path, *parts[relative_path_index:])
        log.debug(f'link: {link_path}')
        link_path.parent.mkdir(parents=True, exist_ok=True)
        link_path.symlink_to(file_path)


def main():
    """Group input data files without modifying the file paths."""
    env = environs.Env()
    data_path = env.path('DATA_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log.debug(f'data_path: {data_path} out_path: {out_path}')
    group(data_path, out_path, relative_path_index)


if __name__ == '__main__':
    main()
