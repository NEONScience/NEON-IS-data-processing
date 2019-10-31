import os
import pathlib
import json
from contextlib import closing
from datetime import datetime

import environs
from structlog import get_logger
import cx_Oracle

import lib.log_config as log_config
import lib.date_formatter as date_formatter
import data_access.threshold_finder as threshold_finder


def write_file(thresholds, out_dir, date_generated):
    """
    Write a threshold file to the given output directory.
    :param thresholds:
    :param out_dir:
    :param date_generated:
    :return:
    """
    pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
    with open(os.path.join(out_dir, 'thresholds.json'), 'w') as outfile:
        threshold_data = {}
        threshold_data.update({'document_date_generated': date_generated})
        threshold_data.update({'thresholds': thresholds})
        json_data = json.dumps(threshold_data, indent=4, sort_keys=False, default=str)
        outfile.write(json_data)


def main():
    env = environs.Env()
    db_url = env('DATABASE_URL')
    out_path = env('OUT_PATH')
    log_level_name = env('LOG_LEVEL')

    log_config.configure(log_level_name)
    log = get_logger()

    log.debug(f'URL: {db_url}')
    log.debug(f'Out path: {out_path}')
    log.debug(f'Log level: {log_level_name}')

    with closing(cx_Oracle.connect(db_url)) as connection:
        thresholds = threshold_finder.find_thresholds(connection)
        date_generated = date_formatter.format(datetime.utcnow())
        write_file(thresholds, out_path, date_generated)


if __name__ == "__main__":
    main()
