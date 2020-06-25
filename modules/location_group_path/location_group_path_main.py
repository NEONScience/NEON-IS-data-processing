#!/usr/bin/env python3
from structlog import get_logger
import environs
from pathlib import Path

import common.log_config as log_config

from location_group_path.data_file_path import DataFilePath
from location_group_path.location_group_path import LocationGroupPath

log = get_logger()


def main():
    """Add the related location group name stored in the location file to the output path."""
    env = environs.Env()
    source_path: Path = env.path('SOURCE_PATH')
    group: str = env.str('GROUP')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    location_index: int = env.int('LOCATION_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} group: {group} out_path: {out_path}')

    data_file_path = DataFilePath(source_type_index=source_type_index,
                                  year_index=year_index,
                                  month_index=month_index,
                                  day_index=day_index,
                                  location_index=location_index,
                                  data_type_index=data_type_index)
    location_group_path = LocationGroupPath(source_path=source_path,
                                            out_path=out_path,
                                            group=group,
                                            data_file_path=data_file_path)
    location_group_path.add_groups_to_paths()


if __name__ == '__main__':
    main()
