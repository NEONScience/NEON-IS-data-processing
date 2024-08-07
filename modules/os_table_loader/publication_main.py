#!/usr/bin/env python3
from contextlib import closing
from pathlib import Path

import environs
from marshmallow.validate import OneOf

from common import log_config
from data_access.db_config_reader import get_connector
from os_table_loader.data.data_loader import get_data_loader
from os_table_loader.publication.publication_config import PublicationConfig, PathConfig
from os_table_loader.publication.publication_file_writer import write_publication_files


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    workbook_path: Path = env.path('WORKBOOK_PATH')
    out_path: Path = env.path('OUT_PATH')
    file_type: str = env.str('FILE_TYPE')
    partial_table_name: str = env.str('PARTIAL_TABLE_NAME')
    input_path_parse_index: int = env.int('INPUT_PATH_PARSE_INDEX')
    data_product_path_index: int = env.int('DATA_PRODUCT_PATH_INDEX')
    year_path_index: int = env.int('YEAR_PATH_INDEX')
    month_path_index: int = env.int('MONTH_PATH_INDEX')
    site_path_index: int = env.int('SITE_PATH_INDEX')
    package_type_path_index: int = env.int('PACKAGE_TYPE_PATH_INDEX')
    db_config_source = env.str('DB_CONFIG_SOURCE',
                               validate=OneOf(['mount', 'environment'],
                                              error='DB_CONFIG_SOURCE must be one of: {choices}'))
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    with closing(get_connector(db_config_source)) as connector:
        data_loader = get_data_loader(connector)
        path_config = PathConfig(input_path=in_path,
                                 workbook_path=workbook_path,
                                 out_path=out_path,
                                 input_path_parse_index=input_path_parse_index,
                                 data_product_path_index=data_product_path_index,
                                 year_path_index=year_path_index,
                                 month_path_index=month_path_index,
                                 site_path_index=site_path_index,
                                 package_type_path_index=package_type_path_index)
        config = PublicationConfig(path_config=path_config,
                                   data_loader=data_loader,
                                   file_type=file_type,
                                   partial_table_name=partial_table_name)
        write_publication_files(config)


if __name__ == '__main__':
    main()
