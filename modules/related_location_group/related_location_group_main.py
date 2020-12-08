#!/usr/bin/env python3
from structlog import get_logger
import environs
from pathlib import Path

import common.log_config as log_config

from related_location_group.related_location_group_config import Config
from related_location_group.related_location_grouper import RelatedLocationGrouper

log = get_logger()


def main() -> None:
    """Group data by related location groups."""
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    group_index: int = env.int('GROUP_INDEX')
    location_index: int = env.int('LOCATION_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log.debug(f'data_path: {data_path} out_path: {out_path}')
    config = Config(data_path=data_path,
                    out_path=out_path,
                    source_type_index=source_type_index,
                    year_index=year_index,
                    month_index=month_index,
                    day_index=day_index,
                    group_index=group_index,
                    location_index=location_index,
                    data_type_index=data_type_index)
    related_location_grouper = RelatedLocationGrouper(config)
    related_location_grouper.group_files()


if __name__ == '__main__':
    main()
