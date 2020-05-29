#!/usr/bin/env python3
import yaml
from pathlib import Path

from structlog import get_logger
import environs

import lib.log_config as log_config

from file_joiner.dictionary_list import DictionaryList
from file_joiner.file_filter import filter_files

log = get_logger()


def get_join_keys(config: dict, out_path: Path, relative_path_index: int):
    """
    Get the file keys and file paths for joining.

    :param config: Configuration settings.
    :param out_path: The root output path for writing joined files.
    :param relative_path_index: Trim the input file paths to this index.
    :return: File keys with associated file paths.
    """
    # store file paths and output paths by key
    file_key_paths = DictionaryList()
    # store all join keys for each configured path for later joining
    join_keys = []
    config_data = yaml.load(config, Loader=yaml.FullLoader)
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
        for file in filter_files(glob_pattern, out_path):
            file = Path(file)
            # create the join key for the file
            join_key = create_join_key(file, join_indices)
            # add the join key to the keys for this configured path
            path_join_keys.add(join_key)
            # create the link path for the file
            link_path = Path(out_path, *file.parts[relative_path_index:])
            # associate the join key, the source file, and the file's output path
            file_key_paths[join_key] = {'file_path': file, 'link_path': link_path}
        # add the join keys for this configured path to the collection for all paths
        join_keys.append(path_join_keys)
    # intersection will pull only the common elements across all sets
    joined_keys = join_keys[0].intersection(*join_keys[1:])
    # return the joined keys and the file paths organized by keys
    return {'joined_keys': joined_keys, 'file_key_paths': file_key_paths}


def create_join_key(file: Path, path_join_indices: list):
    """
    Create a join key for the file by concatenating the path elements
    at the given indices. Join-able files will have the same key.
    :param file: The full file path.
    :param path_join_indices: The indices to pull path elements.
    :return:
    """
    join_key = ''
    path_parts = file.parts
    for index in path_join_indices:
        join_key.join(path_parts[int(index)])
    return join_key


def link_joined_files(joined_keys, file_key_paths):
    """
    Loop over the joined keys, get the files and link them to the output path.

    :param joined_keys: The joined file keys.
    :type joined_keys: set
    :param file_key_paths: The keys and file paths.
    :type file_key_paths: DictionaryList
    :return:
    """
    for key in joined_keys:
        for file_paths in file_key_paths[key]:
            path = file_paths['file_path']
            link_path = file_paths['link_path']
            log.debug(f'file: {path}, link: {link_path}')
            link_path.parent.mkdir(parents=True, exist_ok=True)
            link_path.symlink_to(path)


def build_output_path(file: Path, out_path: Path, output_indices: list):
    """
    Build the output path for a file by extracting the given
    output indices from the file path.

    :param file: The full file path.
    :param out_path: The root output path.
    :param output_indices: Pull file path elements at these indices.
    :return:
    """
    parts = file.parts
    output_path = out_path
    if len(output_indices) == 1:
        index = output_indices[0]
        output_path = Path(output_path, *parts[index:])
    else:
        for index in output_indices:
            output_path = Path(output_path, parts[index])
    return output_path


def main():
    env = environs.Env()
    config = env.str('CONFIG')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    key_files = get_join_keys(config, out_path, relative_path_index)
    joined_keys = key_files.get('joined_keys')
    file_key_paths = key_files.get('file_key_paths')
    link_joined_files(joined_keys, file_key_paths)


if __name__ == '__main__':
    main()
