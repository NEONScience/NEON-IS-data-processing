#!/usr/bin/env python3
from pathlib import Path
from typing import List

import yaml
from structlog import get_logger

import file_joiner.path_filter as path_filter
from file_joiner.dictionary_list import DictionaryList

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
        key_paths = DictionaryList()  # paths (source and link) by key
        keys: List[set] = []
        config_data = yaml.load(self.config, Loader=yaml.FullLoader)
        for input_paths in config_data['input_paths']:
            input_path = input_paths['path']
            glob_pattern: str = input_path['glob_pattern']
            join_indices: List[int] = input_path['join_indices']
            path_keys: set = self.process_path(join_indices, glob_pattern, key_paths)
            keys.append(path_keys)
        # join using intersection to pull common keys
        joined_keys: set = keys[0].intersection(*keys[1:])
        self.link_paths(joined_keys, key_paths)

    def process_path(self, join_indices: List[int], glob_pattern: str, key_paths: DictionaryList) -> set:
        """
        Filter paths, get keys, paths and links.

        :param join_indices: The path indices to create the join keys.
        :param glob_pattern: Unix glob pattern for filtering paths.
        :param key_paths: Paths (source and link) by key.
        :return: The set of keys.
        """
        keys = set()
        for path in path_filter.filter_paths(glob_pattern=glob_pattern, output_path=self.out_path):
            key = self.get_key(path, join_indices)
            keys.add(key)
            link_path = Path(self.out_path, *path.parts[self.relative_path_index:])
            key_paths[key] = (path, link_path)  # add the key and paths
        return keys

    @staticmethod
    def link_paths(keys: set, key_paths: DictionaryList) -> None:
        """
        Loop over the keys and link the paths.

        :param keys: The joined keys.
        :param key_paths: The keys and paths (source and link).
        """
        for key in keys:
            for (path, link) in key_paths[key]:
                link.parent.mkdir(parents=True, exist_ok=True)
                link.symlink_to(path)

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
