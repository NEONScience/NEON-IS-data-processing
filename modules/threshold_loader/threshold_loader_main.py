#!/usr/bin/env python3
from pathlib import Path
from functools import partial
from contextlib import closing

from structlog import get_logger
import environs
from cx_Oracle import connect

import common.log_config as log_config
from data_access.get_thresholds import get_thresholds
from threshold_loader.threshold_loader import load_thresholds


def main() -> None:
    env = environs.Env()
    db_url: str = env.str('DATABASE_URL')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'db_url: {db_url} out_path: {out_path}')

    with closing(connect(db_url)) as connection:
        get_thresholds_partial = partial(get_thresholds, connection=connection)
        load_thresholds(get_thresholds_partial, out_path)


if __name__ == "__main__":
    main()
