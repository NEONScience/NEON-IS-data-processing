#!/usr/bin/env python3
import os

import environs
import structlog

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.target_path as target_path


def filter_directory(in_path, filter_dirs, out_path):
    """
    Link the target directory into the output directory.

    :param in_path: The input path.
    :type in_path: str
    :param filter_dirs: The directories to filter.
    :type filter_dirs: str
    :param out_path: The output path for writing results.
    :type out_path: str
    :return:
    """
    parsed_dirs = parse_dirs(filter_dirs)
    for r, d, f in os.walk(in_path):
        for name in d:
            if not name.startswith('.') and name in parsed_dirs:
                source = os.path.join(r, name)
                destination = target_path.get_path(source, out_path)
                file_linker.link(source, destination)


def parse_dirs(filter_dirs):
    """
    Place filter directories into a list.

    :param filter_dirs: The directories to filter.
    :type filter_dirs: str
    :return:
    """
    dirs = []
    if ',' in filter_dirs:
        dirs = filter_dirs.split(',')
        return dirs
    else:
        dirs.append(filter_dirs)
        return dirs


def main():
    env = environs.Env()
    in_path = env('IN_PATH')
    filter_dirs = env('FILTER_DIR')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} filter_dirs: {filter_dirs} out_dir: {out_path}')
    filter_directory(in_path, filter_dirs, out_path)


if __name__ == '__main__':
    main()
