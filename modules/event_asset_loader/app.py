#!/usr/bin/env python3
from pathlib import Path

import environs
import structlog

from common.file_crawler import crawl
import common.log_config as log_config

log = structlog.get_logger()


def process(data_path: Path, out_path: Path, source_type_index: int, source_id_index: int, filename_index: int):
    """
    Load events from the asset data path.

    :param data_path: The data path.
    :param out_path: The output path for writing results.
    :param source_type_index: The file path source type index.
    :param source_id_index: The file path source ID index.
    :param filename_index: The file path filename index.
    :return:
    """
    for path in crawl(data_path):
        parts = path.parts
        source_type = parts[source_type_index]
        source_id = parts[source_id_index]
        filename = parts[filename_index]
        log.debug(f'file: {filename} type: {source_type} source_id: {source_id}')
        link_filename = f'{source_type}_{source_id}_events.json'
        link_path = Path(out_path, source_type, source_id, link_filename)
        log.debug(f'link_path: {link_path}')
        link_path.parent.mkdir(parents=True, exist_ok=True)
        link_path.symlink_to(path)


def main():
    env = environs.Env()
    source_path = env.path('SOURCE_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    source_id_index = env.int('SOURCE_ID_INDEX')
    filename_index = env.int('FILENAME_INDEX')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} out_path: {out_path}')
    process(source_path, out_path, source_type_index, source_id_index, filename_index)


if __name__ == '__main__':
    main()
