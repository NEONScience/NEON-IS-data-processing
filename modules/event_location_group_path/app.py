#!/usr/bin/env python3
import os
import pathlib

from structlog import get_logger
import environs

import lib.file_linker as file_linker
import lib.file_crawler as file_crawler
import lib.location_file_context as location_file_context
import lib.log_config as log_config
import lib.target_path as target_path

log = get_logger()


def process(source_path, group, out_path):
    """
    Link source files into the output directory with the related location group in the path.
    There must be only one location file under the source path.

    :param source_path: The input path.
    :type source_path: str
    :param group: The group to match in the location files.
    :type group: str
    :param out_path: The output path.
    :type out_path: str
    :return
    """
    paths = []
    group_names = []
    for file_path in file_crawler.crawl(source_path):

        # Parse path elements
        trimmed_path = target_path.trim_path(file_path)
        parts = pathlib.Path(trimmed_path).parts
        source_type = parts[0]
        source_id = parts[1]
        data_type = parts[2]
        filename = parts[3]

        path_parts = {
            "source_type": source_type,
            "source_id": source_id,
            "data_type": data_type,
            "filename": filename
        }

        paths.append({"file_path": file_path, "path_parts": path_parts})

        # Get the full group name from the location file
        if data_type == 'location':
            group_names = location_file_context.get_matching_items(file_path, group)

    if len(group_names) == 0:
        log.error('No location directory found.')
    else:
        link(paths, group_names, out_path)


def link(paths, group_names, out_path):
    """
    Link the paths into the output directory.

    :param paths: The file paths.
    :type paths: list
    :param group_names: The context group names for the location.
    :type group_names: list
    :param out_path: The output path for writing results.
    :type out_path: str
    :return:
    """
    for path in paths:

        file_path = path.get('file_path')
        parts = path.get('path_parts')

        source_type = parts.get("source_type")
        source_id = parts.get("source_id")
        data_type = parts.get("data_type")
        filename = parts.get("filename")

        # Build the output path
        for group_name in group_names:
            log.debug(f'source_type: {source_type} id: {source_id} data_type: {data_type} file: {filename}')
            target_dir = os.path.join(out_path, source_type, group_name, source_id, data_type)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            destination = os.path.join(target_dir, filename)

            # Link the file
            log.debug(f'source: {file_path} destination: {destination}')
            file_linker.link(file_path, destination)


def main():
    """Add the related location group from the location file to the output directory."""
    env = environs.Env()
    source_path = env('SOURCE_PATH')
    group = env('GROUP')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} group: {group} out_path: {out_path}')
    process(source_path, group, out_path)


if __name__ == '__main__':
    main()
