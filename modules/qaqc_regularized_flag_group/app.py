#!/usr/bin/env python3
import os
import pathlib

import environs
from structlog import get_logger

import lib.log_config as log_config
from lib.file_linker import link

log = get_logger()


def group(regularized_files, quality_files):
    """
    Group matching regularized and quality files in the output directory.

    :param regularized_files: Regularized file sources and destinations.
    :type regularized_files: dict
    :param quality_files: Quality file sources and destinations.
    :type quality_files: dict
    :return:
    """
    regularized_keys = set(regularized_files.keys())
    quality_keys = set(quality_files.keys())
    log.debug(f'regularized_keys: {regularized_keys}')
    log.debug(f'quality_keys: {quality_keys}')
    common = regularized_keys.intersection(quality_keys)
    log.debug(f'common: {common}')
    for key in common:
        regularized_paths = regularized_files.get(key)
        quality_paths = quality_files.get(key)
        link(regularized_paths.get('source'), regularized_paths.get('destination'))
        link(quality_paths.get('source'), quality_paths.get('destination'))


def load_files(directory, out_path, relative_path_index):
    """
    Read files in directory and load into a dict of source file path and output paths.

    :param directory: A directory.
    :type directory: str
    :param out_path: The output directory.
    :type out_path: str
    :param relative_path_index: Trim input paths to this index.
    :type relative_path_index: int
    :return: dict containing source file paths and output paths.
    """
    files = {}
    for root, dirs, file_names in os.walk(directory):
        for file_name in file_names:
            source = os.path.join(root, file_name)
            destination = os.path.join(out_path, *pathlib.Path(root).parts[relative_path_index:])
            paths = {'source': source, 'destination': os.path.join(destination, file_name)}
            files.update({destination: paths})
            log.debug(f'adding key: {destination} value: {paths}')
    return files


def main():
    """Group quality and calibration flags."""
    env = environs.Env()
    regularized_path = env.str('REGULARIZED_PATH')
    quality_path = env.str('QUALITY_PATH')
    out_path = env.str('OUT_PATH')
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
