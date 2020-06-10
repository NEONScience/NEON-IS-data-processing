#!/usr/bin/env python3
import structlog
import environs

import common.log_config as log_config
from date_gap_filler_linker.data_gap_filler_linker import DataGapFillerLinker

log = structlog.get_logger()


def main():
    """Read environment variables and create the DataGapFillerLinker object."""
    env = environs.Env()
    in_path = env.path('IN_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    location_index = env.int('LOCATION_INDEX')
    empty_file_suffix = env.str('EMPTY_FILE_SUFFIX')

    log_config.configure(log_level)

    linker = DataGapFillerLinker(in_path, out_path, relative_path_index, location_index, empty_file_suffix)
    linker.link()


if __name__ == '__main__':
    main()
