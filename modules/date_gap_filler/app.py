#!/usr/bin/env python3
import structlog

from date_gap_filler.data_file_handler import link_data_files
from date_gap_filler.location_file_handler import link_location_files
from date_gap_filler.app_config import AppConfig

log = structlog.get_logger()


def main():
    config = AppConfig()
    if config.data_path is not None:
        link_data_files(config)
    if config.location_path is not None:
        link_location_files(config)


if __name__ == '__main__':
    main()
