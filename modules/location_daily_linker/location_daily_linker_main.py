#!/usr/bin/env python3
import environs
import structlog

import common.log_config as log_config
from location_daily_linker.location_daily_linker import link_files


log = structlog.get_logger()


def main():
    env = environs.Env()
    location_path = env.path('LOCATION_PATH')
    out_path = env.path('OUT_PATH')
    schema_index = env.int('SCHEMA_INDEX')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log.info('Processing.')
    link_files(location_path=location_path, out_path=out_path, schema_index=schema_index)


if __name__ == "__main__":
    main()
