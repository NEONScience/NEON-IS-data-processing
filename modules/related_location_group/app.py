import os

from structlog import get_logger
import environs

import lib.log_config as log_config
import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.target_path as target_path

log = get_logger()


def group_source(source_path, out_path):
    """
    Link the source data files into the output directory.
    :param source_path:
    :param out_path:
    :return:
    """
    for file_path in file_crawler.crawl(source_path):
        target = target_path.get_path(file_path, out_path)
        log.debug(f'Source target: {target}')
        file_linker.link(file_path, target)


def group_related(group_path, group_output_path):
    """
    Link the related data files to the output directory.
    :param group_path:
    :param group_output_path:
    :return:
    """
    for file_path in file_crawler.crawl(group_path):
        trimmed_path = target_path.trim_path(file_path)
        target = os.path.join(group_output_path, trimmed_path)
        log.debug(f'Group target: {target}')
        file_linker.link(file_path, target)


def get_related_output_path(source_path, out_path):
    """
    Build the output path for the location related data files.
    :param source_path:
    :param out_path:
    :return:
    """
    target = target_path.get_path(source_path, out_path)
    path = os.path.join(target, 'related_locations')
    return path


def main():
    """
    Group related data sources configured at the same location.
    :return:
    """
    env = environs.Env()
    source_path = env('SOURCE_PATH')
    group_path = env('GROUP_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} group_path: {group_path} out_path: {out_path}')
    group_source(source_path, out_path)
    group_output_path = get_related_output_path(source_path, out_path)
    log.debug(f'group_output_path: {group_output_path}')
    group_related(group_path, group_output_path)


if __name__ == '__main__':
    main()
