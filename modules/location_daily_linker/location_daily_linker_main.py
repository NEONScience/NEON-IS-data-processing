#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config
from location_daily_linker.location_daily_linker_config import Config
from location_daily_linker.location_daily_linker import LocationDailyLinker

log = structlog.get_logger()


def main():
    env = environs.Env()
    location_path: Path = env.path('LOCATION_PATH')
    out_path: Path = env.path('OUT_PATH')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    location_index: int = env.int('LOCATION_INDEX')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    config = Config(location_path=location_path,
                    out_path=out_path,
                    source_type_index=source_type_index,
                    year_index=year_index,
                    month_index=month_index,
                    location_index=location_index)
    location_daily_linker = LocationDailyLinker(config)
    location_daily_linker.link_files()


if __name__ == "__main__":
    main()
