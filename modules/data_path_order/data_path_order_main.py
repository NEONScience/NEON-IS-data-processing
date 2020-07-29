#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import data_path_order.log_config as log_config
from data_path_order.data_path_order_config import Config
from data_path_order.data_path_order import order_paths


def main():
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'data_path: {data_path} out_path: {out_path}')
    config = Config(data_path=data_path,
                    out_path=out_path,
                    source_type_index=source_type_index,
                    year_index=year_index,
                    month_index=month_index,
                    day_index=day_index)
    order_paths(config)


if __name__ == '__main__':
    main()
