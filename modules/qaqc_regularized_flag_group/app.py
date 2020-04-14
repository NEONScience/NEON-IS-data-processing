#!/usr/bin/env python3
import os

import environs
from structlog import get_logger

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.target_path as target_path

log = get_logger()


def group(regularized_dir, quality_dir, out_dir):
    """
    Group matching regularized and quality files in the output directory.
    """
    regularized_files = load_files(regularized_dir, out_dir)
    quality_files = load_files(quality_dir, out_dir)
    regularized_keys = set(regularized_files.keys())
    quality_keys = set(quality_files.keys())
    log.debug(f'regularized_keys: {regularized_keys}')
    log.debug(f'quality_keys: {quality_keys}')
    common = regularized_keys.intersection(quality_keys)
    log.debug(f'common: {common}')
    for key in common:
        regularized_paths = regularized_files.get(key)
        quality_paths = quality_files.get(key)
        file_linker.link(regularized_paths.get('source'), regularized_paths.get('destination'))
        file_linker.link(quality_paths.get('source'), quality_paths.get('destination'))


def load_files(directory, out_dir):
    """
    Read all files in a directory and load them into a dictionary of source file path and output paths.
    :param directory: A directory.
    :param out_dir: The output directory.
    :return: Dictionary containing source file paths and output paths.
    """
    files = {}
    for r, d, f in os.walk(directory):
        for filename in f:
            source = os.path.join(r, filename)
            destination = target_path.get_path(r, out_dir)
            paths = dict(source=source, destination=os.path.join(destination, filename))
            files.update({destination: paths})
            log.debug(f'adding key: {destination} value: {paths}')
    return files


def main():
    """Group quality and calibration flags."""
    env = environs.Env()
    regularized_path = env('REGULARIZED_PATH')
    quality_path = env('QUALITY_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)

    log.debug(f'regularized_path: {regularized_path} quality_path: {quality_path} out_path: {out_path}')
    group(regularized_path, quality_path, out_path)


if __name__ == '__main__':
    main()
