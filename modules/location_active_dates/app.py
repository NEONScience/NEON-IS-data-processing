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
import lib.target_path as target_path


log = structlog.get_logger()


def dates_between(start_date, end_date):
    """
    Generate the dates between the start and end dates.
    :param start_date: Datetime to begin
    :param end_date: Datetime to end
    :return: List of dates.
    """
    delta = end_date - start_date  # as timedelta
    dates = []
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        dates.append(date)
    return dates


def write(out_path, location):
    """
    Write a path and file for each active date '/yyyy/mm/dd/<location name>/<location name>.json'.
    :param out_path: The path for writing files.
    :param location: Geojson object containing the active period and location metadata.
    """
    # create the output directory if it does not exist.
    pathlib.Path(out_path).mkdir(parents=True, exist_ok=True)

    # parse the geojson data for the location name and active periods
    geojson_data = geojson.dumps(location, indent=4, sort_keys=False, default=str)
    json_data = json.loads(geojson_data)
    features = json_data['features']
    properties = features[0]['properties']
    location_name = properties['name']
    active_periods = properties['active-periods']

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
            dir_path = os.path.join(out_path, year, month, day, location_name)
            # create the output directory
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            file_path = os.path.join(dir_path, location_name + '.json')
            # write the file
            with open(file_path, 'w') as outfile:
                outfile.write(geojson_data)


def load(db_url, out_path, context, location_type, cutoff_date):
    """
    Load the locations and write output directories for each location active date.
    :param db_url: A database url.
    :param out_path: The output directory root path.
    :param context: The context to constrain locations read from the database.
    :param location_type: The location type to read from the database.
    :param cutoff_date: The most recent date to create an output directory and file.
    """
    with closing(cx_Oracle.connect(db_url)) as connection:
        locations = named_location_finder.get_type_context(connection, location_type, context, cutoff_date)
        log.debug(f'total locations found: {len(locations)}')
        for location in locations:
            write(out_path, location)


def main():

    # Read configuration
    env = environs.Env()
    context = env('CONTEXT')
    location_type = env('LOCATION_TYPE')
    today = env('tick')
    db_url = env('DATABASE_URL')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'Out path: {out_path}')

    # Parse the input timestamp as the cutoff date.
    if today.startswith('/'):
        path = pathlib.Path(today)
        last_part = target_path.trim_path(path)
        today = str(last_part)
    cutoff_date = datetime.strptime(today, '%Y-%m-%dT%H:%M:%SZ')

    # Load location files and create output directories
    load(db_url, out_path, context, location_type, cutoff_date)


if __name__ == "__main__":
    main()
