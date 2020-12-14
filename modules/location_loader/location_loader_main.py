#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path
from contextlib import closing
from cx_Oracle import connect
from functools import partial

import common.log_config as log_config
from data_access.get_named_locations import get_named_locations
from location_loader.location_loader import load_locations

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    location_type: str = env.str('LOCATION_TYPE')
    db_url: str = env.str('DATABASE_URL')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)

    with closing(connect(db_url)) as connection:
        get_named_locations_partial = partial(get_named_locations, connection=connection, location_type=location_type)
        load_locations(out_path=out_path, get_locations=get_named_locations_partial)


if __name__ == "__main__":
    main()
