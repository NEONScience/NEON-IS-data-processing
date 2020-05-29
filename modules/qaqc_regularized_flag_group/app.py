#!/usr/bin/env python3
import os
from pathlib import Path

import environs
from structlog import get_logger

import lib.log_config as log_config
from lib.file_linker import link

log = get_logger()


def group(regularized_files: dict, quality_files: dict):
    """
    Group matching regularized and quality files in the output directory.

    :param regularized_files: Regularized file sources and destinations.
    :param quality_files: Quality file sources and destinations.
    :return:
    """
    regularized_file_keys = set(regularized_files.keys())
    quality_file_keys = set(quality_files.keys())
    log.debug(f'regularized_keys: {regularized_file_keys}')
    log.debug(f'quality_keys: {quality_file_keys}')
    common_keys = regularized_file_keys.intersection(quality_file_keys)
    log.debug(f'common: {common_keys}')
    for key in common_keys:
        regularized_paths = regularized_files.get(key)
        quality_paths = quality_files.get(key)
        link(regularized_paths.get('file_path'), regularized_paths.get('link_path'))
        link(quality_paths.get('file_path'), quality_paths.get('link_path'))


def load_files(path: Path, out_path: Path, relative_path_index: int):
    """
    Read files and add to dictionary of file paths and link paths.

    :param path: A path containing files.
    :param out_path: The output directory.
    :param relative_path_index: Trim input paths to this index.
    :return: dict containing source file paths and output paths.
    """
    files = {}
    for root, directories, file_names in os.walk(str(path)):
        for file_name in file_names:
            file_path = Path(root, file_name)
            file_key = Path(out_path, *Path(root).parts[relative_path_index:])
            link_path = Path(file_key, file_name)
            file_paths = {'file_path': file_path, 'link_path': link_path}
            files.update({file_key: file_paths})
            log.debug(f'adding key: {file_key} with value: {file_paths}')
    return files


def main():
    """Group quality and calibration flags."""
    env = environs.Env()
    regularized_path = env.path('REGULARIZED_PATH')
    quality_path = env.path('QUALITY_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)

    log.debug(f'regularized_path: {regularized_path} '
              f'quality_path: {quality_path} '
              f'out_path: {out_path}')

    regularized_files = load_files(regularized_path, out_path, relative_path_index)
    quality_files = load_files(quality_path, out_path, relative_path_index)
    group(regularized_files, quality_files)


if __name__ == '__main__':
    main()
