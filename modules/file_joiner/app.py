#!/usr/bin/env python3
import os
import glob
import pathlib

from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker

log = get_logger()


def join(pathname, out_path, relative_path_index):
    """
    Join paths according to the given pathname and
    link all matching files into the output directory.

    :param pathname: The path pattern to match.
    :type pathname: str
    :param out_path: The output path for writing results.
    :type out_path: str
    :param relative_path_index: The starting input path index to include in the output path.
    :type relative_path_index: int
    """
    files = [fn for fn in glob.glob(pathname, recursive=True)
             if not os.path.basename(fn).startswith(out_path) if os.path.isfile(fn)]
    for file_path in files:
        log.debug(f'found matching file: {file_path}')
        path_parts = pathlib.Path(file_path).parts
        target = os.path.join(out_path, *path_parts[relative_path_index:])
        log.debug(f'target: {target}')
        file_linker.link(file_path, target)


def main():
    env = environs.Env()
    pathname = env.str('PATHNAME')
    out_path = env.str('OUT_PATH')
    log_level = env.str('LOG_LEVEL')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log.debug(f'pathname: {pathname}, log_level: {log_level}')
    join(pathname, out_path, relative_path_index)


if __name__ == '__main__':
    main()
