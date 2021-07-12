#!/usr/bin/env python3
from structlog import get_logger
import environs

import common.log_config as log_config
from transformer.transformer import transform

log = get_logger()


def main() -> None:
    env = environs.Env()
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    year_index: int = env.int('YEAR_INDEX')
    log_config.configure(log_level)
    transform(year_index=year_index)

if __name__ == '__main__':
    main()
