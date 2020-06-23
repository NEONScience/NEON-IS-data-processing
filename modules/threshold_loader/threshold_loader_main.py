#!/usr/bin/env python3
from pathlib import Path

from contextlib import closing
from structlog import get_logger
import cx_Oracle
import environs

import common.log_config as log_config
from data_access.threshold_repository import ThresholdRepository
from threshold_loader.threshold_loader import write_file


def main():
    env = environs.Env()
    db_url: str = env.str('DATABASE_URL')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'db_url: {db_url} out_path: {out_path}')
    with closing(cx_Oracle.connect(db_url)) as connection:
        threshold_repository = ThresholdRepository(connection)
        write_file(threshold_repository.get_all, out_path)


if __name__ == "__main__":
    main()
