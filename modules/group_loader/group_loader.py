#!/usr/bin/env python3
from pathlib import Path
from typing import Callable, Iterator

import geojson
import structlog

from data_access.types.group import Group
import data_access.types.geojson_converter as geojson_converter

log = structlog.get_logger()


def load_groups(out_path: Path, get_groups: Callable[[str], Iterator[Group]], group_prefix: str) -> None:
    """
    Write group loader jsons into the output path.

    :param out_path: The path for writing files.
    :param get_groups: A function yielding groups.
    :param group_prefix: group_prefix.
    """
    group_prefix_path = group_prefix
    if group_prefix[-1] == "_":
        group_prefix_path = group_prefix[:-1]
    for group in get_groups(group_prefix=group_prefix):
        if group != []:
            member_name: str = group[0].name
            path = Path(out_path, group_prefix_path, member_name, f'{member_name}.json')
            path.parent.mkdir(parents=True, exist_ok=True)
            geojson_data = geojson_converter.convert_group(group)
            file_data = geojson.dumps(geojson_data, indent=4, sort_keys=True, default=str)
            with open(path, 'w') as file:
                log.debug(f'writing file: {path}')
                file.write(file_data)
