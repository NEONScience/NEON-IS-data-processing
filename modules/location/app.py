#!/usr/bin/env python3
import os
import pathlib

import environs
import structlog
import geojson
import json
import cx_Oracle
from contextlib import closing

import data_access.named_location_finder as named_location_finder
import lib.log_config as log_config

log = structlog.get_logger()


def write_file(out_path, location):
    """
    Write a geojson file to the output path.
    :param out_path: Path for the output file.
    :param location: List containing locations.
    """
    pathlib.Path(out_path).mkdir(parents=True, exist_ok=True)
    formatted = geojson.dumps(location, indent=4, sort_keys=False, default=str)
    json_data = json.loads(formatted)
    features = json_data['features']
    properties = features['properties']
    location_name = properties['name']
    file_name = location_name + '.json'
    file_path = os.path.join(out_path, file_name)
    with open(file_path, 'w') as outfile:
        outfile.write(formatted)


def load(db_url, out_path):
    """Get locations by type and write to file."""
    with closing(cx_Oracle.connect(db_url)) as connection:
        locations = named_location_finder.get_by_type(connection, 'CONFIG')
        for location in locations:
            log.debug(f'Processing location {location}')
            write_file(out_path, location)


def main():
    env = environs.Env()
    out_path = env('OUT_PATH')
    db_url = env('DATABASE_URL')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'Out path: {out_path}')
    load(db_url, out_path)


if __name__ == "__main__":
    main()
