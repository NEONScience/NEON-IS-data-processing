#!/usr/bin/env python3
import environs
import structlog
import cx_Oracle
from contextlib import closing

from data_access.named_location_repository import NamedLocationRepository
import common.date_formatter as date_formatter
import common.log_config as log_config
from location_loader.location_loader import LocationLoader

log = structlog.get_logger()


def main():
    env = environs.Env()
    location_type = env.str('LOCATION_TYPE')
    cutoff_date_path = env.path('tick')
    db_url = env.str('DATABASE_URL')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    log_config.configure(log_level)

    cutoff_date = date_formatter.parse_date_path(cutoff_date_path)

    with closing(cx_Oracle.connect(db_url)) as connection:
        named_location_repository = NamedLocationRepository(connection)
        location_loader = LocationLoader(named_location_repository)
        location_loader.load_files(location_type, cutoff_date, out_path)


if __name__ == "__main__":
    main()
