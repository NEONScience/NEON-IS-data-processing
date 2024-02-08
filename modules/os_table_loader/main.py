#!/usr/bin/env python3
from contextlib import closing
from pathlib import Path

import environs
from marshmallow.validate import OneOf

from common import log_config
from data_access.db_config_reader import get_connector
from os_table_loader.data.data_loader import get_data_loader
from os_table_loader.file_writer import write_files


def main() -> None:
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    file_type: str = env.str('FILE_TYPE')
    partial_table_name: str = env.str('PARTIAL_TABLE_NAME')
    db_config_source = env.str('DB_CONFIG_SOURCE',
                               validate=OneOf(['mount', 'environment'],
                               error='DB_CONFIG_SOURCE must be one of: {choices}'))
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    with closing(get_connector(db_config_source)) as connector:
        data_loader = get_data_loader(connector)
        if partial_table_name is not None:
            write_files(out_path, data_loader, file_type, partial_table_name)


if __name__ == '__main__':
    main()
