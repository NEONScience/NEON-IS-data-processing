#!/usr/bin/env python3
from pathlib import Path
from typing import Callable, Iterator

import geojson
import structlog
import sys

from data_access.types.group import Group
import data_access.types.geojson_converter as geojson_converter
from common.err_datum import err_datum_path

log = structlog.get_logger()


def load_groups(out_path: Path, err_path: Path, get_groups: Callable[[str], Iterator[Group]], group_prefix: str) -> None:
    """
    Write group loader jsons into the output path.

    :param out_path: The path for writing files.
    :param err_path: The error directory, i.e., errored.
    :param get_groups: A function yielding groups.
    :param group_prefix: group_prefix.
    """
    # DirErrBase: the user specified error directory, i.e., /tmp/out/errored

    DirErrBase = Path(err_path)
    dataDir_routed = Path("")
    group_prefix_path = group_prefix
    if group_prefix[-1] == "_":
        group_prefix_path = group_prefix[:-1]
    for group in get_groups(group_prefix=group_prefix):
        if group != []:
            member_name: str = group[0].name
            path = Path(out_path, group_prefix_path, member_name, f'{member_name}.json')
            path.parent.mkdir(parents=True, exist_ok=True)
            dataDir_routed = path.parent
            try:
                geojson_data = geojson_converter.convert_group(group)
                file_data = geojson.dumps(geojson_data, indent=4, sort_keys=True, default=str)
                with open(path, 'w') as file:
                    log.debug(f'writing file: {path}')
                    file.write(file_data)
            except:
                err_msg = sys.exc_info()
                err_datum_path(err=err_msg,DirDatm=str(dataDir_routed),DirErrBase=DirErrBase,
                               RmvDatmOut=True,DirOutBase=out_path)
