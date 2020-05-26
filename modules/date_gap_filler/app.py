#!/usr/bin/env python3
from date_gap_filler.data_file_handler import link_data_files
from date_gap_filler.location_file_handler import link_location_files
from date_gap_filler.app_config import AppConfig

import lib.log_config


def main():
    config = AppConfig()
    # configure log
    lib.log_config.configure(config.log_level)
    if config.data_path is not None:
        link_data_files(config)
    if config.location_path is not None:
        link_location_files(config)


if __name__ == '__main__':
    main()
