#!/usr/bin/env python3
from structlog import get_logger
import environs

import common.log_config as log_config
from grouper.grouper import group_files

log = get_logger()


def main():
    """Group input data files without modifying the file paths."""
    env = environs.Env()
    data_path = env.path('DATA_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    log.debug(f'data_path: {data_path} out_path: {out_path}')
    group_files(path=data_path, out_path=out_path, relative_path_index=relative_path_index)


if __name__ == '__main__':
    main()
