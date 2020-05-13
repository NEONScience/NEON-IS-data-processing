#!/usr/bin/env python3
import os
import pathlib
import json
from datetime import datetime, timedelta

import geojson
import environs
import structlog
import cx_Oracle
from contextlib import closing

import data_access.named_location_finder as named_location_finder
import lib.date_formatter as date_formatter
import lib.log_config as log_config


log = structlog.get_logger()


def dates_between(start_date, end_date):
    """
    Generate the dates between the start and end dates.

    :param start_date: Datetime to begin
    :type start_date: datetime
    :param end_date: Datetime to end
    :type end_date: datetime
    :return: list of dates.
    """
    delta = end_date - start_date  # as timedelta
    dates = []
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        dates.append(date)
    return dates


def load(db_url, out_path, location_type, cutoff_date):
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
        named_locations = named_location_finder.get_by_type(connection, location_type, cutoff_date=cutoff_date)
        log.debug(f'total locations found: {len(named_locations)}')
        for named_location in named_locations:

            # parse the geojson data for the location name and active periods
            geojson_data = geojson.dumps(named_location, indent=4, sort_keys=False, default=str)
            json_data = json.loads(geojson_data)
            features = json_data['features']
            properties = features[0]['properties']
            location_name = properties['name']
            active_periods = properties['active-periods']

            schema_name = named_location_finder.get_schema_name(connection, location_name)
            if schema_name is not None:
                # loop over active periods and write directories and files for each date
                for period in active_periods:
                    start_date = period['start_date']
                    end_date = period['end_date']
                    dates = dates_between(date_formatter.parse(start_date), date_formatter.parse(end_date))
                    for date in dates:
                        datetime_obj = datetime(date.year, date.month, date.day)
                        year = datetime_obj.strftime('%Y')
                        month = datetime_obj.strftime('%m')
                        day = datetime_obj.strftime('%d')
                        dir_path = os.path.join(out_path, schema_name, year, month, day, location_name)
                        # create the output directory
                        if not os.path.exists(dir_path):
                            os.makedirs(dir_path)
                        file_path = os.path.join(dir_path, location_name + '.json')
                        # write the file
                        with open(file_path, 'w') as outfile:
                            outfile.write(geojson_data)


def main():
    env = environs.Env()
    location_type = env.str('LOCATION_TYPE')
    today = env.str('tick')
    db_url = env.str('DATABASE_URL')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'Out path: {out_path}')

    # Parse the input timestamp as the cutoff date.
    if today.startswith('/'):
        path = pathlib.Path(today)
        parts = path.parts[3:]
        trimmed_path = os.path.join(*parts)
        today = str(trimmed_path)
    cutoff_date = datetime.strptime(today, '%Y-%m-%dT%H:%M:%SZ')

    # Load location files and create output directories
    load(db_url, out_path, location_type, cutoff_date)


if __name__ == "__main__":
    main()
