#!/usr/bin/env python3
import os
import pathlib

from structlog import get_logger
import environs

from lib.file_linker import link
from lib.file_crawler import crawl
import lib.location_file_context as location_file_context
import lib.log_config as log_config

log = get_logger()


def get_paths(source_path,
              group,
              source_type_index,
              source_id_index,
              data_type_index,
              filename_index):
    """
    Link source files into the output directory with the related location group in the path.
    There must be only one location file under the source path.

    :param source_path: The input path.
    :type source_path: str
    :param group: The group to match in the location files.
    :type group: str
    :param source_type_index: The file path source type index.
    :type source_type_index: int
    :param source_id_index: The file path source ID index.
    :type source_id_index: int
    :param data_type_index: The file path data type index.
    :type data_type_index: int
    :param filename_index: The file path filename index.
    :type filename_index: int
    :return
    """
    paths = []
    group_names = []
    for file_path in crawl(source_path):
        parts = pathlib.Path(file_path).parts
        source_type = parts[source_type_index]
        source_id = parts[source_id_index]
        data_type = parts[data_type_index]
        filename = parts[filename_index]

        path_parts = {
            "source_type": source_type,
            "source_id": source_id,
            "data_type": data_type,
            "filename": filename
        }

        paths.append({"file_path": file_path, "path_parts": path_parts})

        # get the full group name from the location file
        if data_type == 'location':
            group_names = location_file_context.get_matching_items(file_path, group)

    if len(group_names) == 0:
        log.error('No location directory found.')
    return {'paths': paths, 'group_names': group_names}


def group_paths(paths, group_names, out_path):
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

        # build the output path
        for group_name in group_names:
            log.debug(f'source_type: {source_type} id: {source_id} data_type: {data_type} file: {filename}')
            target_dir = os.path.join(out_path, source_type, group_name, source_id, data_type)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            destination = os.path.join(target_dir, filename)

            # link files
            log.debug(f'source: {file_path} destination: {destination}')
            link(file_path, destination)


def main():
    """Add the related location group from the location file to the output directory."""
    env = environs.Env()
    source_path = env.str('SOURCE_PATH')
    group = env.str('GROUP')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    source_id_index = env.int('SOURCE_ID_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    filename_index = env.int('FILENAME_INDEX')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} group: {group} out_path: {out_path}')
    results = get_paths(source_path,
                        group,
                        source_type_index,
                        source_id_index,
                        data_type_index,
                        filename_index)
    paths = results.get('paths')
    group_names = results.get('group_names')
    group_paths(paths, group_names, out_path)


if __name__ == '__main__':
    main()
