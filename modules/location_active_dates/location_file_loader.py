#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime

import geojson
import environs
import structlog
import cx_Oracle
from contextlib import closing

from data_access.named_location_repository import NamedLocationRepository
import common.date_formatter as date_formatter
import common.log_config as log_config


log = structlog.get_logger()


def load(db_url: str, out_path: Path, location_type: str, cutoff_date: datetime):
    """
    Write a file for each location.

    :param db_url: A database url.
    :param out_path: The output directory path.
    :param location_type: The location type to read from the database.
    :param cutoff_date: The last date to create a file.
    """
    with closing(cx_Oracle.connect(db_url)) as connection:
        named_location_repository = NamedLocationRepository(connection)
        named_locations = named_location_repository.get_by_type(location_type, cutoff_date=cutoff_date)
        for named_location in named_locations:
            geojson_data = geojson.dumps(named_location, indent=4, sort_keys=False, default=str)
            json_data = json.loads(geojson_data)
            features = json_data['features']
            properties = features[0]['properties']
            location_name = properties['name']
            schema_name = named_location_repository.get_schema_name(location_name)
            if schema_name is not None:
                path = Path(out_path, schema_name, location_name, f'{location_name}.json')
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, 'w') as location_file:
                    location_file.write(geojson_data)


def main():
    env = environs.Env()
    location_type = env.str('LOCATION_TYPE')
    cutoff_date_path = env.path('tick')
    db_url = env.str('DATABASE_URL')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    log_config.configure(log_level)

    cutoff_date = date_formatter.parse_date_path(cutoff_date_path)
    load(db_url, out_path, location_type, cutoff_date)


if __name__ == "__main__":
    main()
