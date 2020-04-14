#!/usr/bin/env python3
import os
import glob

from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.target_path as target_path

log = get_logger()


def join(pathname, out_path):
    """
    Link all matching files into the output directory.
    :param pathname: The path pattern to match.
    :param out_path: The output path for writing results.
    """
    files = [fn for fn in glob.glob(pathname, recursive=True)
             if not os.path.basename(fn).startswith(out_path) if os.path.isfile(fn)]
    for file in files:
        log.debug(f'processing path: {file}')
        log.debug(f'found matching file: {file}')
        target = target_path.get_path(file, out_path)
        log.debug(f'target: {target}')
        file_linker.link(file, target)


def main():
    """
    Join paths according to the given pathname.
    """
    env = environs.Env()
    pathname = env('PATHNAME')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'pathname: {pathname}, log_level: {log_level}')
    join(pathname, out_path)


if __name__ == '__main__':
    main()
