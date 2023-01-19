#!/usr/bin/env python3
from pathlib import Path
from typing import Callable, Iterator

import geojson
import structlog

from data_access.types.srf import Srf
import data_access.types.geojson_converter as geojson_converter

log = structlog.get_logger()


def load_srfs(out_path: Path, get_srfs: Callable[[str], Iterator[Srf]], group_prefix: str) -> None:
    """
    Write srf loader jsons into the output path.

    :param out_path: The path for writing files.
    :param get_groups: A function yielding groups.
    :param group_prefix: group_prefix.
    """
    group_prefix_path = group_prefix
    if group_prefix[-1] == "_":
        group_prefix_path = group_prefix[:-1]
    srf_name_str: str = "_science_review_flags"
    srf_file_name: str = (group_prefix_path,f'{srf_name_str}.json')
    with open(Path(out_path,srf_file_name), 'w') as file:
        srfs = []
        for srf in get_srfs(group_prefix=group_prefix):
            srfs.append(srf._asdict())
        srf_data = {}
        srf_data.update({'science_review_flags': srfs})
        json_data = json.dumps(srf_data, indent=4, sort_keys=False, default=str)
        file.write(json_data)

