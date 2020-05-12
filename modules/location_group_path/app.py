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
              year_index,
              month_index,
              day_index,
              location_index,
              data_type_index):
    """
    Link source files into the output directory with the related location group in the path.
    There must be only one location file under the source path.

    :param source_path: The input path.
    :type source_path: str
    :param group: The group to match in the location files.
    :type group: str
    :param source_type_index: The input path index of the source type.
    :type source_type_index: int
    :param year_index: The input path index of the year.
    :type year_index: int
    :param month_index: The input path index of the month.
    :type month_index: int
    :param day_index: The input path index of the day.
    :type day_index: int
    :param location_index: The input path index of the location.
    :type location_index: int
    :param data_type_index: The input path index of the data type.
    :type data_type_index: int
    """
    paths = []
    group_names = []
    for file_path in crawl(source_path):
        parts = pathlib.Path(file_path).parts
        source_type = parts[source_type_index]
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        location = parts[location_index]
        data_type = parts[data_type_index]
        remainder = parts[data_type_index + 1:]  # everything after the data type
        # put path parts into dictionary
        path_parts = {
            "source_type": source_type,
            "year": year,
            "month": month,
            "day": day,
            "location": location,
            "data_type": data_type,
            "remainder": remainder
        }
        # add the original file path and path parts to paths
        paths.append({"file_path": file_path, "path_parts": path_parts})

        # get the location context group name from the location file
        if data_type == 'location':
            group_names = location_file_context.get_matching_items(file_path, group)

    # location context group name was not found!
    if len(group_names) == 0:
        log.error(f'No location directory found for groups {group_names}.')

    return {'paths': paths, 'group_names': group_names}


def link_paths(paths, group_names, out_path):
    """
    Loop through the files and link into the output directory including the location
    context group name in the path.

    :param paths: File paths to link.
    :type paths: list
    :param group_names: A List of associated location context group names.
    :type group_names: list
    :param out_path: The output directory for writing.
    :type out_path: str
    :return:
    """
    for path in paths:
        file_path = path.get('file_path')
        parts = path.get('path_parts')
        source_type = parts.get("source_type")
        year = parts.get("year")
        month = parts.get("month")
        day = parts.get("day")
        location = parts.get("location")
        data_type = parts.get("data_type")
        remainder = parts.get("remainder")
        for group_name in group_names:
            target_dir = os.path.join(out_path, source_type, year, month, day, group_name, location, data_type)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            destination = os.path.join(target_dir, *remainder[0:])
            log.debug(f'source: {file_path} destination: {destination}')
            link(file_path, destination)


def main():
    """Add the related location group name stored in the location file to the output path."""
    env = environs.Env()
    source_path = env.str('SOURCE_PATH')
    group = env.str('GROUP')
    out_path = env.str('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    location_index = env.int('LOCATION_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} group: {group} out_path: {out_path}')
    results = get_paths(source_path,
                        group,
                        source_type_index,
                        year_index,
                        month_index,
                        day_index,
                        location_index,
                        data_type_index)
    paths = results.get('paths')
    group_names = results.get('group_names')
    link_paths(paths, group_names, out_path)


if __name__ == '__main__':
    main()
