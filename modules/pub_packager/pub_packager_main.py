#!/usr/bin/env python3
from structlog import get_logger
import environs

import common.log_config as log_config
from pub_packager.pub_packager import pub_package

log = get_logger()


def main() -> None:
    env = environs.Env()
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    out_path = env.path('OUT_PATH')
    err_path = env.path('ERR_PATH_PACKAGER')
    data_path = env.path('DATA_PATH')
    product_index: int = env.int('PRODUCT_INDEX')
    publoc_index: int = env.int('PUBLOC_INDEX')
    date_index: int = env.int('DATE_INDEX')
    date_index_length: int = env.int('DATE_INDEX_LENGTH')
    sort_index: int = env.int('SORT_INDEX')
    log_config.configure(log_level)
    pub_package(data_path=data_path,
                out_path=out_path, 
                err_path=err_path,
                product_index=product_index,
                publoc_index=publoc_index,
                date_index=date_index,
                date_index_length=date_index_length,
                sort_index=sort_index)


if __name__ == '__main__':
    main()
