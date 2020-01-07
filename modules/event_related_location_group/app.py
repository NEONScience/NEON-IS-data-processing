import pathlib
import os

import environs
import structlog

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = structlog.get_logger()


def group_data(data_path, out_path):
    """Write data and event files into output path."""
    target_root = None
    for file_path in file_crawler.crawl(data_path):
        trimmed_path = target_path.trim_path(file_path)
        parts = trimmed_path.parts
        year = parts[0]
        month = parts[1]
        day = parts[2]
        group_name = parts[3]
        source_type = parts[4]
        location = parts[5]
        data_type = parts[6]
        filename = parts[7]
        target_root = os.path.join(out_path, year, month, day, group_name)
        data_target_path = os.path.join(target_root, source_type, location, data_type, filename)
        file_linker.link(file_path, data_target_path)
    return target_root


def group_events(event_path, target_root):
    reference_group = pathlib.Path(target_root).name
    for file_path in file_crawler.crawl(event_path):
        trimmed_path = target_path.trim_path(file_path)
        parts = pathlib.Path(trimmed_path).parts
        source_type = parts[0]
        group_name = parts[1]
        source_id = parts[2]
        data_type = parts[3]
        filename = parts[4]
        event_target = os.path.join(target_root, source_type, source_id, data_type, filename)
        log.debug(f'event_target: {event_target}')
        if group_name == reference_group:
            file_linker.link(file_path, event_target)


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    event_path = env('EVENT_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'data_dir: {data_path} event_dir: {event_path} out_dir: {out_path}')
    target_root_path = group_data(data_path, out_path)
    group_events(event_path, target_root_path)


if __name__ == '__main__':
    main()
