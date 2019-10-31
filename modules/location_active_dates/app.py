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
    :param end_date: Datetime to end
    :return: List of dates.
    """
    delta = end_date - start_date  # as timedelta
    days = []
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        days.append(day)
    return days


def write(out_path, location):
    """
    Write a file for each active period '/yyyy/mm/dd/location_name'
    :param out_path: The path for writing files.
    :param location: Geojson containing the active period and location metadata.
    """
    pathlib.Path(out_path).mkdir(parents=True, exist_ok=True)
    geojson_data = geojson.dumps(location, indent=4, sort_keys=False, default=str)
    json_dict = json.loads(geojson_data)
    features = json_dict['features']
    properties = features[0]['properties']
    location_name = properties['name']
    active_periods = properties['active-periods']
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
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            file_path = os.path.join(dir_path, location_name + '.json')
            with open(file_path, 'w') as outfile:
                outfile.write(geojson_data)


def load(db_url, out_path, context, location_type):
    with closing(cx_Oracle.connect(db_url)) as connection:
        locations = named_location_finder.get_type_context(connection, location_type, context)
        log.debug(f'total locations found: {len(locations)}')
        for location in locations:
            write(out_path, location)


def main():
    env = environs.Env()
    context = env('CONTEXT')
    location_type = env('LOCATION_TYPE')
    db_url = env('DATABASE_URL')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'Out path: {out_path}')
    load(db_url, out_path, context, location_type)


if __name__ == "__main__":
    main()
