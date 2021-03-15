#!/usr/bin/env python3
from typing import NamedTuple, List, Iterator, Optional

import yaml


class InputPath(NamedTuple):
    path_name: str
    glob_pattern: str
    join_indices: List[int]
    outer_join: Optional[str]


def parse_config(config: str) -> Iterator[InputPath]:
    config_data = yaml.load(config, Loader=yaml.FullLoader)
    for paths in config_data['input_paths']:
        path = paths['path']
        path_name: str = path['name']
        glob_pattern: str = path['glob_pattern']
        join_indices: List[int] = path['join_indices']
        outer_join: str = path.get('outer_join')  # optional entry
        yield InputPath(path_name=path_name,
                        glob_pattern=glob_pattern,
                        join_indices=join_indices,
                        outer_join=outer_join)
