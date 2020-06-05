#!/usr/bin/env python3
from pathlib import Path
import json

from contextlib import closing
from structlog import get_logger
import cx_Oracle
import environs

import lib.log_config as log_config
from data_access.threshold_repository import ThresholdRepository


def write_threshold_file(thresholds: list, out_path: Path):
    """
    Write a threshold file into the given output path.

    :param thresholds: The thresholds.
    :param out_path: The path for writing results.
    """
    out_path.mkdir(parents=True, exist_ok=True)
    threshold_file_path = Path(out_path, 'thresholds.json')
    with open(threshold_file_path, 'w') as threshold_file:
        threshold_data = {}
        threshold_data.update({'thresholds': thresholds})
        json_data = json.dumps(threshold_data, indent=4, sort_keys=False, default=str)
        threshold_file.write(json_data)


def main():
    env = environs.Env()
    db_url = env.str('DATABASE_URL')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')

    log_config.configure(log_level)
    log = get_logger()

    log.info('Processing.')
    log.debug(f'db_url: {db_url} out_path: {out_path}')

    with closing(cx_Oracle.connect(db_url)) as connection:
        threshold_repository = ThresholdRepository(connection)
        thresholds = threshold_repository.get_thresholds()
        write_threshold_file(thresholds, out_path)


if __name__ == "__main__":
    main()
