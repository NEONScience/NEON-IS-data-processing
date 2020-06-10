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


class LocationLoader(object):

    def __init__(self, named_location_repository: NamedLocationRepository):
        self.named_location_repository = named_location_repository

    def load_files(self, location_type: str, cutoff_date: datetime, out_path: Path):
        """
        Write a file for each location.

        :param location_type: The location type to read from the database.
        :param cutoff_date: The last date to create a file.
        :param out_path: The output directory path.
        """
        named_locations = self.named_location_repository.get_by_type(location_type, cutoff_date=cutoff_date)
        for named_location in named_locations:
            geojson_data = geojson.dumps(named_location, indent=4, sort_keys=False, default=str)
            json_data = json.loads(geojson_data)
            features = json_data['features']
            properties = features[0]['properties']
            location_name = properties['name']
            schema_name = self.named_location_repository.get_schema_name(location_name)
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

    with closing(cx_Oracle.connect(db_url)) as connection:
        named_location_repository = NamedLocationRepository(connection)
        location_loader = LocationLoader(named_location_repository)
        location_loader.load_files(location_type, cutoff_date, out_path)


if __name__ == "__main__":
    main()
