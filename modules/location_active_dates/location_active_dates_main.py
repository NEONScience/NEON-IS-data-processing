#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config
from location_active_dates.location_active_dates import link_location_files


log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    location_path: Path = env.path('LOCATION_PATH')
    out_path: Path = env.path('OUT_PATH')
    schema_index: int = env.int('SCHEMA_INDEX')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    link_location_files(location_path=location_path, out_path=out_path, schema_index=schema_index)


if __name__ == "__main__":
    main()
