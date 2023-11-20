#!/usr/bin/env python3
from pathlib import Path
from typing import Callable, Iterator, Set

import geojson
import structlog
import sys

from data_access.types.named_location import NamedLocation
import data_access.types.geojson_converter as geojson_converter
from common.err_datum import err_datum_path

log = structlog.get_logger()


def load_locations(out_path: Path, err_path: Path, get_locations: Callable[[str], Iterator[NamedLocation]], source_type: str) -> None:
    """
    Write location files into the output path.
    :param out_path: The path for writing files.
    :param err_path: The error directory, i.e., errored.
    :param get_locations: A function yielding named locations.
    :param source_type: sensor type.
    """
    # DirErrBase: the user specified error directory, i.e., /tmp/out/errored
    DirErrBase = Path(err_path)
    dataDir_routed = Path("")
    for named_location in get_locations(source_type=source_type):
        schema_names: Set = named_location.schema_names
        location_name: str = named_location.name
        for schema_name in schema_names:
            path = Path(out_path, schema_name, location_name, f'{location_name}.json')
            path.parent.mkdir(parents=True, exist_ok=True)
            dataDir_routed = path.parent
            try:
                geojson_data = geojson_converter.convert_named_location(named_location)
                file_data = geojson.dumps(geojson_data, indent=4, sort_keys=True, default=str)
                with open(path, 'w') as file:
                    log.debug(f'writing file: {path}')
                    file.write(file_data)
            except:
                err_msg = sys.exc_info()
                err_datum_path(err=err_msg,DirDatm=str(dataDir_routed),DirErrBase=DirErrBase,
                               RmvDatmOut=True,DirOutBase=out_path)