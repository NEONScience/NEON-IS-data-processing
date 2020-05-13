#!/usr/bin/env python3
import os
import pathlib

import structlog

from lib.file_linker import link
from lib.file_crawler import crawl
import lib.location_file_context as location_file_context

log = structlog.get_logger()


def get_files_by_source(in_path, source_id_index, data_type_index):
    """
    Organize all files in the input directory by source (sensor).

    :param in_path: The input path.
    :type in_path: str
    :param source_id_index: The path index for the source ID.
    :type source_id_index: int
    :param data_type_index: The path index for the data type.
    :type data_type_index: int
    """
    source_files = {}
    for file in crawl(in_path):
        parts = pathlib.Path(file).parts
        source_id = parts[source_id_index]
        data_type = parts[data_type_index]
        log.debug(f'source_id: {source_id} data_type: {data_type}')
        files = source_files.get(source_id)
        if files is None:  # first iteration for this source (sensor)
            files = []
        files.append({data_type: file})
        source_files.update({source_id: files})
    return source_files


def match_files_by_context(source_files, context):
    """
    Group files by location context group and write to output if the
    location file context matches the given context.

    :param source_files: File paths by data type.
    :type source_files: dict
    :param context: The context to match.
    :type context: str
    """
    matching_file_paths = []
    for source in source_files:
        file_paths = source_files.get(source)
        for path in file_paths:
            for data_type in path:
                file = path.get(data_type)
                if data_type == 'location' and location_file_context.match(file, context):
                    matching_file_paths.append(file_paths)
    return matching_file_paths


def link_matching_files(matching_file_paths,
                        out_path,
                        source_type_index,
                        year_index,
                        month_index,
                        day_index,
                        source_id_index,
                        data_type_index):
    """
    Pull files by the data type and link into output directory.

    :param matching_file_paths: Files organized by data type.
    :type matching_file_paths dict
    :param out_path: The output path.
    :type out_path: str
    :param source_type_index: The path index for the source type.
    :type source_type_index: int
    :param year_index: The path index for the year.
    :type year_index: int
    :param month_index: The path index for the month.
    :type month_index: int
    :param day_index: The path index for the day.
    :type day_index: int
    :param source_id_index: The path index for the source ID.
    :type source_id_index: int
    :param data_type_index: The path index for the data type.
    :type data_type_index: int
    """
    for file_paths in matching_file_paths:
        for data_type_files in file_paths:
            for data_type in data_type_files:
                file = data_type_files.get(data_type)
                parts = pathlib.Path(file).parts
                source_type = parts[source_type_index]
                year = parts[year_index]
                month = parts[month_index]
                day = parts[day_index]
                source_id = parts[source_id_index]
                file_data_type = parts[data_type_index]
                destination = os.path.join(out_path, source_type, year, month, day,
                                           source_id, file_data_type, *parts[data_type_index + 1:])
                log.debug(f'file: {file} destination: {destination}')
                link(file, destination)
