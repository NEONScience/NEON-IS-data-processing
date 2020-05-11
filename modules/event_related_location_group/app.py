#!/usr/bin/env python3
import pathlib
import os

import environs
import structlog

import lib.log_config as log_config
from lib.file_linker import link
from lib.file_crawler import crawl

log = structlog.get_logger()


def group_data(data_path,
               out_path,
               year_index,
               month_index,
               day_index,
               group_name_index,
               source_type_index,
               location_index,
               data_type_index,
               filename_index):
    """
    Write data and event files into output path.

    :param data_path: The path to the data files.
    :type data_path: str
    :param out_path: The output path for writing results.
    :type out_path: str
    :param year_index: The file path year index.
    :type year_index: int
    :param month_index: The file path month index.
    :type month_index: int
    :param day_index: The file path day index.
    :type day_index: int
    :param group_name_index: The file path group name index.
    :type group_name_index: int
    :param source_type_index: The file path source type index.
    :type source_type_index: int
    :param location_index: The file path location index.
    :type location_index: int
    :param data_type_index: The file path data type index.
    :type data_type_index: int
    :param filename_index: The file path filename index.
    :type filename_index: int
    :return:
    """
    target_root = None
    for file_path in crawl(data_path):
        parts = pathlib.Path(file_path).parts
        year = parts[year_index]
        month = parts[month_index]
        day = parts[day_index]
        group_name = parts[group_name_index]
        source_type = parts[source_type_index]
        location = parts[location_index]
        data_type = parts[data_type_index]
        filename = parts[filename_index]
        target_root = os.path.join(out_path, year, month, day, group_name)
        target = os.path.join(target_root, source_type, location, data_type, filename)
        link(file_path, target)
    return target_root


def group_events(event_path,
                 target_root,
                 source_type_index,
                 group_name_index,
                 source_id_index,
                 data_type_index,
                 filename_index):
    """
    Group the event files into the target directory.

    :param event_path: The path to the event files.
    :type event_path: str
    :param target_root: The root output path.
    :type target_root: str
    :param source_type_index: The file path source type index.
    :type source_type_index: int
    :param group_name_index: The file path group name index.
    :type group_name_index: int
    :param source_id_index: The file path source ID index.
    :type source_id_index: int
    :param data_type_index: The file path data type index.
    :type data_type_index: int
    :param filename_index: The file path filename index.
    :type filename_index: int
    :return:
    """
    reference_group = pathlib.Path(target_root).name
    for file_path in crawl(event_path):
        parts = pathlib.Path(file_path).parts
        source_type = parts[source_type_index]
        group_name = parts[group_name_index]
        source_id = parts[source_id_index]
        data_type = parts[data_type_index]
        filename = parts[filename_index]
        target = os.path.join(target_root, source_type, source_id, data_type, filename)
        log.debug(f'event_target: {target}')
        if group_name == reference_group:
            link(file_path, target)


def main():
    env = environs.Env()
    data_path = env.str('DATA_PATH')
    event_path = env.str('EVENT_PATH')
    out_path = env.str('OUT_PATH')
    log_level = env.str('LOG_LEVEL')
    data_year_index = env.int('DATA_YEAR_INDEX')
    data_month_index = env.int('DATA_MONTH_INDEX')
    data_day_index = env.int('DATA_DAY_INDEX')
    data_group_name_index = env.int('DATA_GROUP_NAME_INDEX')
    data_source_type_index = env.int('DATA_SOURCE_TYPE_INDEX')
    data_location_index = env.int('DATA_LOCATION_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    data_filename_index = env.int('DATA_FILENAME_INDEX')
    event_source_type_index = env.int('EVENT_SOURCE_TYPE_INDEX')
    event_group_name_index = env.int('EVENT_GROUP_NAME_INDEX')
    event_source_id_index = env.int('EVENT_SOURCE_ID_INDEX')
    event_data_type_index = env.int('EVENT_DATA_TYPE_INDEX')
    event_filename_index = env.int('EVENT_FILENAME_INDEX')
    log_config.configure(log_level)
    log.debug(f'data_dir: {data_path} event_dir: {event_path} out_dir: {out_path}')
    target_root_path = group_data(data_path,
                                  out_path,
                                  data_year_index,
                                  data_month_index,
                                  data_day_index,
                                  data_group_name_index,
                                  data_source_type_index,
                                  data_location_index,
                                  data_type_index,
                                  data_filename_index)
    group_events(event_path,
                 target_root_path,
                 event_source_type_index,
                 event_group_name_index,
                 event_source_id_index,
                 event_data_type_index,
                 event_filename_index)


if __name__ == '__main__':
    main()
