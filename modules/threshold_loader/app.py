#!/usr/bin/env python3
from pathlib import Path
import json
from contextlib import closing
from datetime import datetime

import environs
from structlog import get_logger
import cx_Oracle

import lib.log_config as log_config
import lib.date_formatter as date_formatter
import data_access.threshold_finder as threshold_finder


def write_file(thresholds: list, out_dir: Path, date_generated: str):
    """
    Write a threshold file into the given output path.

    :param thresholds: The threshold file.
    :param out_dir: The path for writing results.
    :param date_generated: The date generated.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(Path(out_dir, 'thresholds.json'), 'w') as threshold_file:
        threshold_data = {}
        threshold_data.update({'document_date_generated': date_generated})
        threshold_data.update({'thresholds': thresholds})
        json_data = json.dumps(threshold_data, indent=4, sort_keys=False, default=str)
        threshold_file.write(json_data)


def main():
    env = environs.Env()
    db_url = env.str('DATABASE_URL')
    out_path = env.path('OUT_PATH')
    log_level_name = env.log_level('LOG_LEVEL')

    log_config.configure(log_level_name)
    log = get_logger()

    log.debug(f'URL: {db_url}')
    log.debug(f'Out path: {out_path}')
    log.debug(f'Log level: {log_level_name}')

    with closing(cx_Oracle.connect(db_url)) as connection:
        thresholds = threshold_finder.find_thresholds(connection)
        date_generated = date_formatter.convert(datetime.utcnow())
        write_file(thresholds, out_path, date_generated)


if __name__ == "__main__":
    main()
