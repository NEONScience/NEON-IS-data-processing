#!/usr/bin/env python3
import os
import glob
import json
import pathlib

from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.target_path as target_path

log = get_logger()


class DictList(dict):
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
    :param out_path: The output path for writing results.
    :type out_path: str
    """
    files = [fn for fn in glob.glob(pattern, recursive=True)
             if not os.path.basename(fn).startswith(out_path) if os.path.isfile(fn)]
    return files


def join(config, out_path):
    file_key_dict = DictList()
    file_key_sets = []
    json_data = json.loads(config)
    for input_path in json_data['input_paths']:
        input_name = input_path['name']
        log.debug(f'input_name: {input_name}')
        path_pattern = input_path['path_pattern']
        path_join_indices = input_path['path_join_indices']
        filtered_files = filter_files(path_pattern, out_path)
        file_key_set = set()
        for file in filtered_files:
            parts = pathlib.Path(file).parts
            key = ''
            for index in path_join_indices:
                key += parts[int(index)]  # generate the file key
            file_key_set.add(key)  # add the key
            file_key_dict[key] = file  # store keys with related files
        # if not len(file_key_set) == 0:
        file_key_sets.append(file_key_set)  # add all the file keys for the input
    joined_keys = set.intersection(*file_key_sets)  # join_keys(file_key_sets)
    write_files(joined_keys, file_key_dict, out_path)


def write_files(joined_keys, file_keys, out_path):
    log.debug(f'joined_keys: {joined_keys}')
    log.debug(f'file_keys: {file_keys}')
    for key in file_keys.keys():
        if key in joined_keys:
            files = file_keys[key]
            for file in files:
                target = target_path.get_path(file, out_path)
                log.debug(f'target: {target}')
                file_linker.link(file, target)


def main():
    env = environs.Env()
    config = env('CONFIG')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    join(config, out_path)


if __name__ == '__main__':
    main()
