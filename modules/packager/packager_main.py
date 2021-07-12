#!/usr/bin/env python3
from structlog import get_logger
import environs

import common.log_config as log_config
from packager.packager import package

log = get_logger()


def main() -> None:
    env = environs.Env()
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    prefix_index: int = env.int('PREFIX_INDEX')
    prefix_length: int = env.int('PREFIX_LENGTH')
    sort_index: int = env.int('SORT_INDEX')
    log_config.configure(log_level)
    package(prefix_index=prefix_index, prefix_length=prefix_length, sort_index=sort_index)

if __name__ == '__main__':
    main()
