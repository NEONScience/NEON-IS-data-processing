#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime

import geojson
import structlog

from data_access.named_location_repository import NamedLocationRepository

log = structlog.get_logger()


class LocationLoader(object):

    def __init__(self, named_location_repository: NamedLocationRepository):
        self.named_location_repository = named_location_repository

    def load_files(self, *, location_type: str, cutoff_date: datetime, out_path: Path):
        """
        Write a file for each location.

        :param location_type: The location type to read.
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
