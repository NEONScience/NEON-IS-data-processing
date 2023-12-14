#!/usr/bin/env python3
from contextlib import closing
from pathlib import Path

import environs

from common import log_config
from data_access.db_config_reader import read_from_mount, read_from_environment
from data_access.db_connector import DbConnector
from maintenance_table_loader.data_reader import get_data_reader
from maintenance_table_loader.loader import load_files


def get_source_key():
    return 'DB_CONFIG_SOURCE'



def main() -> None:
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    file_type: str = env.str('FILE_TYPE')
    db_config_source = env.str(get_source_key())
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    if db_config_source == 'mount':
        db_config = read_from_mount(Path('/var/db_secret'))
    elif db_config_source == 'environment':
        db_config = read_from_environment()
    else:
        msg = f"{get_source_key()} '{db_config_source}' should be set to 'mount' or 'environment'."
        raise SystemExit(msg)
    with closing(DbConnector(db_config)) as connector:
        data_reader = get_data_reader(connector)
        load_files(out_path, data_reader, file_type)


if __name__ == '__main__':
    main()
