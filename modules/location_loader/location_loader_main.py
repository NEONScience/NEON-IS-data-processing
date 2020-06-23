#!/usr/bin/env python3
import environs
import structlog
import cx_Oracle
from contextlib import closing
from pathlib import Path


import common.date_formatter as date_formatter
import common.log_config as log_config
from data_access.named_location_repository import NamedLocationRepository
import location_loader.location_loader as location_loader

log = structlog.get_logger()


def main():
    env = environs.Env()
    location_type: str = env.str('LOCATION_TYPE')
    cutoff_date_path: Path = env.path('tick')
    db_url: str = env.str('DATABASE_URL')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)

    cutoff_date = date_formatter.parse_date_path(cutoff_date_path)

    with closing(cx_Oracle.connect(db_url)) as connection:
        named_location_repository = NamedLocationRepository(connection)
        location_loader.write_files(location_type=location_type, cutoff_date=cutoff_date, out_path=out_path,
                                    get_locations=named_location_repository.get_by_type,
                                    get_schema_name=named_location_repository.get_schema_name)


if __name__ == "__main__":
    main()
