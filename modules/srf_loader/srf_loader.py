#!/usr/bin/env python3
from pathlib import Path
import json
from typing import Callable, Iterator

import structlog

from data_access.types.srf import Srf

log = structlog.get_logger()


def load_srfs(out_path: Path, get_srfs: Callable[[str], Iterator[Srf]], group_prefix: str) -> None:
    """
    Write science review flag data to JSON files in the output path.

    :param out_path: The path for writing files, /GROUP/GROUP_science_review_flags.json
    :param get_srfs: A function yielding science review flag entries.
    :param group_prefix: A group prefix, i.e., rel-humidity_ or surfacewater-physical_.
    """
    srf_name: str = "_science_review_flags"
    srfs = []
    for srf in get_srfs(group_prefix=group_prefix):
        srfs.append(srf._asdict())
    if not srfs:
        print("Science review is not written")
    else:
        s = 0
        s_len = len(srfs)
        group_names = []
        while s < s_len:
            gname: str = srfs[s]["group_name"]
            if gname not in group_names:
                group_names.append(gname)
            s = s + 1
        for group_name in group_names:
            s = -1
            srfs_group = []
            while s < (s_len-1):
                s = s + 1
                if group_name == srfs[s]["group_name"]:
                    srfs_group.append(srfs[s])
                    srf_file_name: str = group_name + f'{srf_name}.json'
                    path = Path(out_path, group_name, srf_file_name)
                    path.parent.mkdir(parents=True, exist_ok=True)
                    srf_data = {}
                    srf_data.update({'science_review_flags': srfs_group})
                    json_data = json.dumps(srf_data, indent=4, sort_keys=True, default=str)
                    with open(path, 'w') as file:
                        file.write(json_data)
        
