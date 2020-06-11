#!/usr/bin/env python3
from structlog import get_logger
import environs

import common.log_config as log_config
from location_group_path.location_group_path import get_paths, link_paths

log = get_logger()


def main():
    """Add the related location group name stored in the location file to the output path."""
    env = environs.Env()
    source_path = env.path('SOURCE_PATH')
    group = env.str('GROUP')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    location_index = env.int('LOCATION_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} group: {group} out_path: {out_path}')
    results = get_paths(source_path,
                        group,
                        source_type_index,
                        year_index,
                        month_index,
                        day_index,
                        location_index,
                        data_type_index)
    paths = results.get('paths')
    group_names = results.get('group_names')
    link_paths(paths, group_names, out_path)


if __name__ == '__main__':
    main()
