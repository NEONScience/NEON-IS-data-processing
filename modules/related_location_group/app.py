import os
import pathlib

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
    :param source_path: The source path.
    :param out_path: The output directory.
    """
    for file_path in file_crawler.crawl(source_path):
        target = target_path.get_path(file_path, out_path)
        log.debug(f'Source target: {target}')
        file_linker.link(file_path, target)


def group_related(related_path_variables, group_output_path):
    """
    Link related data and location files into the output directory.
    :param related_path_variables: Related path variable names containing directory paths.
    :param group_output_path: The output path for related data.
    """
    for related_path_variable in related_path_variables:
        path = os.environ[related_path_variable]
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

            minimal_path = os.path.join(source_type, location, data_type, filename)

            target = os.path.join(group_output_path, minimal_path)
            log.debug(f'Group target: {target}')
            file_linker.link(file_path, target)


def get_related_output_path(source_path, out_path):
    """
    Build the output path for the location related data files.
    :param source_path: The source path.
    :param out_path: The output path for writing source data.
    """
    target = target_path.get_path(source_path, out_path)
    path = os.path.join(target, 'related_locations')
    return path


def main():
    """
    Group related data sources configured at the same location.
    """
    env = environs.Env()
    source_path = env('SOURCE_PATH')
    related_inputs = env('RELATED_INPUTS')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} related_inputs: {related_inputs} out_path: {out_path}')
    group_source(source_path, out_path)
    group_output_path = get_related_output_path(source_path, out_path)
    log.debug(f'group_output_path: {group_output_path}')
    # Check for multiple inputs
    if ',' in related_inputs:
        related_path_variables = related_inputs.split(',')
    else:
        related_path_variables = ['RELATED_INPUTS']
    group_related(related_path_variables, group_output_path)


if __name__ == '__main__':
    main()
