#!/usr/bin/env python3
from pathlib import Path
import json
from typing import Callable, Iterator

import geojson
import structlog

log = structlog.get_logger()


def write_files(*, location_type: str, out_path: Path,
                get_locations: Callable[[str], Iterator[geojson.FeatureCollection]],
                get_schema_name: Callable[[str], str]) -> None:
    """
    Write a file for each named location.

    :param location_type: The named location type to read.
    :param out_path: The output directory path.
    :param get_locations: Function returning an Iterator of named locations in GEOJson format.
    :param get_schema_name: Function returning the Avro schema name bound to the named location.
    """
    for named_location in get_locations(location_type):
        geojson_data = geojson.dumps(named_location, indent=4, sort_keys=False, default=str)
        json_data = json.loads(geojson_data)
        properties = json_data['properties']
        location_name = properties['name']
        schema_name = get_schema_name(location_name)
        if schema_name is not None:
            path = Path(out_path, schema_name, location_name, f'{location_name}.json')
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as location_file:
                location_file.write(geojson_data)
