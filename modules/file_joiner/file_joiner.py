#!/usr/bin/env python3
from pathlib import Path
from typing import List, Iterator, Tuple

from structlog import get_logger

import file_joiner.path_filter as path_filter
from file_joiner.dictionary_list import DictionaryList
from file_joiner.config_parser import InputPath, parse_config

log = get_logger()


class FileJoiner:

    def __init__(self, *, config: str, out_path: Path, relative_path_index: int) -> None:
        """
        Constructor.

        :param config: Yaml configuration.
        :param out_path: The output path for writing joined files.
        :param relative_path_index: Trim input file paths to this index.
        """
        self.config = config
        self.out_path = out_path
        self.relative_path_index = relative_path_index

    def join(self) -> None:
        """Join paths by common key."""
        key_paths = DictionaryList()
        keys: List[set] = []
        for join_path in parse_config(self.config):
            keys.append(self.get_keys(join_path, key_paths))
        # join using intersection to pull common keys
        joined_keys: set = keys[0].intersection(*keys[1:])
        for input_path_name, path in self.get_joined_paths(joined_keys, key_paths):
            self.link_path(path)

    def get_keys(self, input_path: InputPath, key_paths: DictionaryList) -> set:
        """
        Filter paths, then loop through and associate keys and paths for joining.

        :param input_path: The join path.
        :param key_paths: Paths by key.
        :return: The set of keys.
        """
        keys = set()
        for path in path_filter.filter_paths(glob_pattern=input_path.glob_pattern, output_path=self.out_path):
            key = self.get_key(path, input_path.join_indices)
            keys.add(key)
            key_paths[key] = (input_path.path_name, path)
        return keys

    def link_path(self, path: Path):
        link_path = Path(self.out_path, *path.parts[self.relative_path_index:])
        link_path.parent.mkdir(parents=True, exist_ok=True)
        if not link_path.exists():
            link_path.symlink_to(path)

    @staticmethod
    def get_key(path: Path, join_indices: list) -> str:
        """
        Create a key by concatenating path elements at the join indices.
        Paths to join will have the same key.

        :param path: A path.
        :param join_indices: Path element indices to use for the key.
        :return: The key.
        """
        key = ''
        parts = path.parts
        for index in join_indices:
            key.join(parts[index])
        return key

    @staticmethod
    def get_joined_paths(keys: set, key_paths: DictionaryList) -> Iterator[Tuple[str, Path]]:
        """
        Loop through the joined keys and pull the associated paths.

        :param keys: The joined keys.
        :param key_paths: Paths by key.
        """
        for key in keys:
            for (input_path_name, path) in key_paths[key]:
                yield input_path_name, path
