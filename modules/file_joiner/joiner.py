#!/usr/bin/env python3
import os
import glob
import yaml
import pathlib

from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker

log = get_logger()


class DictList(dict):
    """
    Class to automatically add new dictionary values with the same keys into a list.
    """
    def __setitem__(self, key, value):
        try:
            # assumes there is a list on the key
            self[key].append(value)
        except KeyError:  # there is no key
            super(DictList, self).__setitem__(key, value)
        except AttributeError:  # it is not a list
            super(DictList, self).__setitem__(key, [self[key], value])


def filter_files(pattern, out_path):
    """
    Filter files according to the given pattern.

    :param pattern: The path pattern to match.
    :type pattern: str
    :param out_path: The output path so it can be ignored when evaluating files.
    :type out_path: str
    """
    files = [fn for fn in glob.glob(pattern, recursive=True)
             if not os.path.basename(fn).startswith(out_path) if os.path.isfile(fn)]
    return files


def get_join_keys(config, out_path):
    """
    Get the file keys and file paths to join.

    :param config: A config specification of paths to evaluate.
    :param out_path: The root output path for writing joined files.
    :return: dict of file keys and file paths to join.
    """
    file_key_paths = DictList()
    file_key_sets = []

    config_data = yaml.load(config, Loader=yaml.FullLoader)
    for paths in config_data['paths']:
        path = paths['path']
        path_name = path['name']
        log.debug(f'path_name: {path_name}')
        path_pattern = path['path_pattern']
        path_join_indices = path['path_join_indices']

        filtered_files = filter_files(path_pattern, out_path)
        file_key_set = set()
        for file in filtered_files:
            parts = pathlib.Path(file).parts
            key = ''
            for index in path_join_indices:
                key += parts[int(index)]  # generate the file key
            file_key_set.add(key)  # add the key
            file_key_paths[key] = file  # store keys with related files
        file_key_sets.append(file_key_set)  # add all the file keys for the input
    first_set = file_key_sets[0]  # get the first key set
    joined_keys = first_set.intersection(*file_key_sets[1:])  # add all key sets but the first
    return {'joined_keys': joined_keys, 'file_key_paths': file_key_paths}


def write_files(joined_keys, file_key_paths, out_path, relative_path_index):
    """
    Loop over the joined keys, get the file paths and write them into the output directory.

    :param joined_keys:
    :param file_key_paths:
    :param out_path:
    :param relative_path_index:
    :return:
    """
    log.debug(f'joined_keys: {joined_keys}')
    log.debug(f'file_key_paths: {file_key_paths}')
    for key in file_key_paths.keys():
        if key in joined_keys:
            file_paths = file_key_paths[key]
            for file_path in file_paths:
                path_parts = pathlib.Path(file_path).parts
                target = os.path.join(out_path, *path_parts[relative_path_index:])
                log.debug(f'target: {target}')
                file_linker.link(file_path, target)


def main():
    env = environs.Env()
    config = env.str('CONFIG')
    out_path = env.str('OUT_PATH')
    log_level = env.str('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    results = get_join_keys(config, out_path)
    joined_keys = results.get('joined_keys')
    file_key_paths = results.get('file_key_paths')
    write_files(joined_keys, file_key_paths, out_path, relative_path_index)


if __name__ == '__main__':
    main()
