#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime, timedelta

import geojson
import environs
import structlog
import cx_Oracle
from contextlib import closing

from data_access.named_location_finder import get_by_type, get_schema_name
import lib.date_formatter as date_formatter
import lib.log_config as log_config


log = structlog.get_logger()


def find_dates_between(start_date: datetime, end_date: datetime):
    """
    Generate all dates between the start and end dates.

    :param start_date: Datetime to start generating dates.
    :param end_date: Datetime to finish generating dates.
    :return: list of all dates between the start and end dates.
    """
    delta = end_date - start_date
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        yield date


def load(db_url: str, out_path: Path, location_type: str, cutoff_date: datetime):
    """
    Write a path and file for each active date '/yyyy/mm/dd/<location name>/<location name>.json'.

    :param db_url: A database url.
    :type db_url: str
    :param out_path: The output directory root path.
    :type out_path: str
    :param location_type: The location type to read from the database.
    :type location_type: str
    :param cutoff_date: The most recent date to create an output directory and file.
    :type cutoff_date: datetime
    """
    with closing(cx_Oracle.connect(db_url)) as connection:
        for named_location in get_by_type(connection, location_type, cutoff_date=cutoff_date):
            # parse the geojson data for the location name and active periods
            geojson_data = geojson.dumps(named_location, indent=4, sort_keys=False, default=str)
            json_data = json.loads(geojson_data)
            features = json_data['features']
            properties = features[0]['properties']
            location_name = properties['name']
            active_periods = properties['active-periods']
            schema_name = get_schema_name(connection, location_name)
            if schema_name is not None:
                # loop over active periods and write directories and files for each date
                for period in active_periods:
                    start_date = date_formatter.parse(period['start_date'])
                    end_date = date_formatter.parse(period['end_date'])
                    for date in find_dates_between(start_date, end_date):
                        d = datetime(date.year, date.month, date.day)
                        year = d.strftime('%Y')
                        month = d.strftime('%m')
                        day = d.strftime('%d')
                        file_path = Path(out_path, schema_name, year, month, day,
                                         location_name, f'{location_name}.json')
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(file_path, 'w') as location_file:
                            location_file.write(geojson_data)


def parse_cutoff_date(cutoff_date_path: Path):
    """
    Parse the input timestamp as the cutoff date.

    :param cutoff_date_path: The path containing the timestamp file.
    :return: datetime
    """
    extracted_date = str(*cutoff_date_path.parts[3:])
    return datetime.strptime(extracted_date, '%Y-%m-%dT%H:%M:%SZ')


def main():
    env = environs.Env()
    location_type = env.str('LOCATION_TYPE')
    cutoff_date_path = env.path('tick')
    db_url = env.str('DATABASE_URL')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'Out path: {out_path}')

    cutoff_date = parse_cutoff_date(cutoff_date_path)
    load(db_url, out_path, location_type, cutoff_date)


if __name__ == "__main__":
    main()
