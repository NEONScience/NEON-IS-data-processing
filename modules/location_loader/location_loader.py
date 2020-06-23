#!/usr/bin/env python3
from pathlib import Path
import json
from datetime import datetime
from typing import Callable, Iterator

import geojson
import structlog

log = structlog.get_logger()


def write_files(*, location_type: str, cutoff_date: datetime, out_path: Path,
                get_locations: Callable[[str, datetime], Iterator[geojson.FeatureCollection]],
                get_schema_name: Callable[[str], str]):
    """
    Write a file for each named location.

    :param location_type: The named location type to read.
    :param cutoff_date: The last date to create a file.
    :param out_path: The output directory path.
    :param get_locations: Function returning an Iterator of named locations in GEOJson format.
    :param get_schema_name: Function returning the Avro schema name bound to the named location.
    """
    for named_location in get_locations(location_type, cutoff_date):
        geojson_data = geojson.dumps(named_location, indent=4, sort_keys=False, default=str)
        json_data = json.loads(geojson_data)
        features = json_data['features']
        properties = features[0]['properties']
        location_name = properties['name']
        schema_name = get_schema_name(location_name)
        if schema_name is not None:
            path = Path(out_path, schema_name, location_name, f'{location_name}.json')
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as location_file:
                location_file.write(geojson_data)
