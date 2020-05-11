#!/usr/bin/env python3
import os
import pathlib

import environs
import structlog

import lib.log_config as log_config
from lib.file_linker import link
from lib.file_crawler import crawl

log = structlog.get_logger()


def process(data_path, out_path, source_type_index, source_id_index, filename_index):
    """
    Load events from the asset data path.

    :param data_path: The data path.
    :type data_path: str
    :param out_path: The output path for writing results.
    :type out_path: str
    :param source_type_index: The souce type index in file paths.
    :type source_type_index: int
    :param source_id_index: The source ID index in input file paths.
    :type source_id_index: int
    :param filename_index: The filename index in input file paths.
    :type filename_index: int
    :return:
    """
    for file_path in crawl(data_path):
        parts = pathlib.Path(file_path).parts
        source_type = parts[source_type_index]
        source_id = parts[source_id_index]
        filename = parts[filename_index]
        log.debug(f'source filename: {filename}')
        log.debug(f'source type: {source_type} source_id: {source_id}')
        output_filename = source_type + '_' + source_id + '_events.json'
        output_path = os.path.join(out_path, source_type, source_id, output_filename)
        log.debug(f'output_path: {output_path}')
        if not os.path.exists(output_path):
            link(file_path, output_path)


def main():
    env = environs.Env()
    source_path = env('SOURCE_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    source_id_index = env.int('SOURCE_ID_INDEX')
    filename_index = env.int('FILENAME_INDEX')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} out_path: {out_path}')
    process(source_path, out_path, source_type_index, source_id_index, filename_index)


if __name__ == '__main__':
    main()
