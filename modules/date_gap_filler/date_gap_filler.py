#!/usr/bin/env python3
import common.log_config

from date_gap_filler.data_file_linker import DataFileLinker
from date_gap_filler.location_file_linker import LocationFileLinker
from date_gap_filler.app_config import AppConfig


def main():
    config = AppConfig()

    common.log_config.configure(config.log_level)

    if config.data_path is not None:
        data_file_linker = DataFileLinker(config)
        data_file_linker.link_files()

    if config.location_path is not None:
        location_file_linker = LocationFileLinker(config)
        location_file_linker.link_files()


if __name__ == '__main__':
    main()
