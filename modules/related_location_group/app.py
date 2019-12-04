import os
import pathlib

from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = get_logger()


def group_related(related_paths, out_path):
    """
    Link related data and location files into the output directory.
    :param related_paths: Related path variable names containing directory paths.
    :param out_path: The output path for related data.
    """
    if ',' in related_paths:
        related_paths = related_paths.split(',')
    log.debug(f'related_paths: {related_paths}')
    for related_path in related_paths:
        log.debug(f'related_path: {related_path}')
        path = os.environ[related_path]
        for file_path in file_crawler.crawl(path):
            trimmed_path = target_path.trim_path(file_path)
            parts = pathlib.Path(trimmed_path).parts
            source_type = parts[0]
            year = parts[1]
            month = parts[2]
            day = parts[3]
            group = parts[4]
            location = parts[5]
            data_type = parts[6]
            filename = parts[7]
            base_output_path = os.path.join(out_path, year, month, day, group)
            target = os.path.join(base_output_path, source_type, location, data_type, filename)
            log.debug(f'File target: {target}')
            file_linker.link(file_path, target)


def main():
    """
    Group data by related location group.
    """
    env = environs.Env()
    related_paths = env('RELATED_PATHS')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'related_inputs: {related_paths} out_path: {out_path}')
    group_related(related_paths, out_path)


if __name__ == '__main__':
    main()
