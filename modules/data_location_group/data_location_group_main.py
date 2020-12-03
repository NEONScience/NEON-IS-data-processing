#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config

from data_location_group.data_location_group_config import Config
from data_location_group.data_location_grouper import DataLocationGrouper

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    location_path: Path = env.path('LOCATION_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    log.debug(f'data_dir: {data_path} location_dir: {location_path} out_dir: {out_path}')
    common.log_config.configure(log_level)
    config = Config(data_path=data_path,
                    location_path=location_path,
                    out_path=out_path,
                    source_type_index=source_type_index,
                    year_index=year_index,
                    month_index=month_index,
                    day_index=day_index,
                    source_id_index=source_id_index)
    data_location_grouper = DataLocationGrouper(config)
    data_location_grouper.group_files()


if __name__ == '__main__':
    main()
