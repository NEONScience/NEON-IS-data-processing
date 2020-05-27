#!/usr/bin/env python3
from pathlib import Path

import structlog

import lib.location_file_context as location_file_context

log = structlog.get_logger()


def get_files_by_source(in_path: Path, source_id_index: int, data_type_index: int):
    """
    Organize all files in the input directory by source (sensor).

    :param in_path: The input path.
    :param source_id_index: The path index for the source ID.
    :param data_type_index: The path index for the data type.
    """
    source_files = {}
    for path in in_path.rglob('*'):
        if path.is_file():
            parts = path.parts
            source_id = parts[source_id_index]
            data_type = parts[data_type_index]
            log.debug(f'source_id: {source_id} data_type: {data_type}')
            files = source_files.get(source_id)
            if files is None:  # first iteration for this source (sensor)
                files = []
            files.append({data_type: path})
            source_files.update({source_id: files})
    return source_files


def match_files_by_context(source_files: dict, context: str):
    """
    Group files by location context group and write to output if the
    location file context matches the given context.

    :param source_files: File paths by data type.
    :param context: The context to match.
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


def link_matching_files(matching_file_paths: dict,
                        out_path: Path,
                        source_type_index: int,
                        year_index: int,
                        month_index: int,
                        day_index: int,
                        source_id_index: int,
                        data_type_index: int):
    """
    Pull files by the data type and link into output directory.

    :param matching_file_paths: Files organized by data type.
    :param out_path: The output path.
    :param source_type_index: The path index for the source type.
    :param year_index: The path index for the year.
    :param month_index: The path index for the month.
    :param day_index: The path index for the day.
    :param source_id_index: The path index for the source ID.
    :param data_type_index: The path index for the data type.
    """
    for file_paths in matching_file_paths:
        for data_type_files in file_paths:
            for data_type in data_type_files:
                path = data_type_files.get(data_type)
                parts = path.parts
                source_type = parts[source_type_index]
                year = parts[year_index]
                month = parts[month_index]
                day = parts[day_index]
                source_id = parts[source_id_index]
                file_data_type = parts[data_type_index]
                link_path = Path(out_path, source_type, year, month, day,
                                 source_id, file_data_type, *parts[data_type_index + 1:])
                link_path.parent.mkdir(parents=True, exist_ok=True)
                log.debug(f'file: {path} link_path: {link_path}')
                link_path.symlink_to(path)
