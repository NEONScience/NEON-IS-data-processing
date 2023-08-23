#!/usr/bin/env python3
from structlog import get_logger
import environs
from pathlib import Path

import common.log_config as log_config
from pub_transformer.pub_transformer import pub_transform

log = get_logger()


def main() -> None:
    env = environs.Env()
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    workbook_path: Path = env.path('WORKBOOK_PATH')
    product_index: int = env.int('PRODUCT_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    group_metadata_dir: str = env.str('GROUP_METADATA_DIR')
    data_path_parse_index: int = env.int('DATA_PATH_PARSE_INDEX')
    log_config.configure(log_level)
    pub_transform(data_path=data_path, 
                  out_path=out_path, 
                  workbook_path=workbook_path,
                  product_index=product_index,
                  year_index=year_index,
                  data_type_index=data_type_index,
                  group_metadata_dir=group_metadata_dir,
                  data_path_parse_index=data_path_parse_index)


if __name__ == '__main__':
    main()
