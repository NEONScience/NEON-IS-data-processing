#!/usr/bin/env python3
import os

import environs
import structlog

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = structlog.get_logger()


def process(data_path, out_path):
    """
    Load events from the asset data path.

    :param data_path: The data path.
    :type data_path: str
    :param out_path: The output path for writing results.
    :type out_path: str
    :return:
    """
    for file_path in file_crawler.crawl(data_path):
        trimmed_path = target_path.trim_path(file_path)
        parts = trimmed_path.parts
        source_type = parts[0]
        source_id = parts[1]
        filename = parts[2]
        log.debug(f'source filename: {filename}')
        log.debug(f'source type: {source_type} source_id: {source_id}')
        output_filename = source_type + '_' + source_id + '_events.json'
        output_path = os.path.join(out_path, source_type, source_id, output_filename)
        log.debug(f'output_path: {output_path}')
        if not os.path.exists(output_path):
            file_linker.link(file_path, output_path)


def main():
    env = environs.Env()
    source_path = env('SOURCE_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} out_path: {out_path}')
    process(source_path, out_path)


if __name__ == '__main__':
    main()
