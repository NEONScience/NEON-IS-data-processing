#!/usr/bin/env python3
import yaml
from pathlib import Path

from structlog import get_logger

from file_joiner.filter_files import filter_files
from file_joiner.dictionary_list import DictionaryList

log = get_logger()


class FileJoiner(object):

    def __init__(self, *, config: dict, out_path: Path, relative_path_index: int):
        """
        Constructor.

        :param config: Configuration settings parsed from yaml.
        :param out_path: The root output path for writing joined files.
        :param relative_path_index: Trim the input file paths to this index.
        """
        self.config = config
        self.out_path = out_path
        self.relative_path_index = relative_path_index

    def join_files(self):
        key_files = self.get_join_keys()
        joined_keys = key_files.get('joined_keys')
        file_key_paths = key_files.get('file_key_paths')
        self.link_joined_files(joined_keys, file_key_paths)

    def get_join_keys(self):
        """
        Get the file keys and file paths for joining.

        :return: File paths organized by key.
        """
        # store file paths and output paths by key
        file_key_paths = DictionaryList()
        # store all join keys for each configured path for later joining
        join_keys = []
        config_data = yaml.load(self.config, Loader=yaml.FullLoader)
        # loop over each configured input path
        for input_paths in config_data['input_paths']:
            input_path = input_paths['path']
            # the glob pattern for filtering
            glob_pattern = input_path['glob_pattern']
            # the join indices for path elements used in joining
            join_indices = input_path['join_indices']
            # use a set for the joining keys to avoid duplicates
            path_join_keys = set()
            # loop over the filtered files
            for file in filter_files(glob_pattern=glob_pattern, output_path=self.out_path):
                file = Path(file)
                # create the join key for the file
                join_key = self.create_join_key(file, join_indices)
                # add the join key to the keys for this configured path
                path_join_keys.add(join_key)
                # create the link path for the file
                link_path = Path(self.out_path, *file.parts[self.relative_path_index:])
                # associate the join key, the source file, and the file's output path
                file_key_paths[join_key] = {'file_path': file, 'link_path': link_path}
            # add the join keys for this configured path to the collection for all paths
            join_keys.append(path_join_keys)
        # intersection will pull only the common elements across all sets
        joined_keys = join_keys[0].intersection(*join_keys[1:])
        # return the joined keys and the file paths organized by keys
        return {'joined_keys': joined_keys, 'file_key_paths': file_key_paths}

    @staticmethod
    def create_join_key(file: Path, join_indices: list):
        """
        Create a join key for the file by concatenating the path elements
        at the given indices. Join-able files will have the same key.

        :param file: The full file path.
        :param join_indices: The indices to pull path elements.
        :return: The key.
        """
        key = ''
        path_parts = file.parts
        for index in join_indices:
            key.join(path_parts[int(index)])
        return key

    @staticmethod
    def link_joined_files(joined_keys: set, file_key_paths: DictionaryList):
        """
        Loop over the joined keys, get the files and link them to the output path.

        :param joined_keys: The joined file keys.
        :param file_key_paths: The keys and file paths.
        """
        for key in joined_keys:
            for file_paths in file_key_paths[key]:
                path = file_paths['file_path']
                link_path = Path(file_paths['link_path'])
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
