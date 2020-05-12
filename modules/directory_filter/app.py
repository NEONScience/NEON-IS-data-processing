#!/usr/bin/env python3

import environs
import structlog

import lib.log_config as log_config

from directory_filter.filter import filter_directory


def main():
    env = environs.Env()
    in_path = env.str('IN_PATH')
    filter_dirs = env.list('FILTER_DIR')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} filter_dirs: {filter_dirs} out_dir: {out_path}')
    filter_directory(in_path, filter_dirs, out_path, relative_path_index)


if __name__ == '__main__':
    main()
