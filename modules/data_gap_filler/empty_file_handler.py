import os
import pathlib
import structlog

from lib.file_crawler import crawl
from lib.file_linker import link


log = structlog.get_logger()


def get_paths(empty_files_path, file_type_index):
    """
    Get a path for each type of empty file.

    :param empty_files_path: The directory containing empty files.
    :type empty_files_path: str
    :param file_type_index: The index of the file type (e.g. 'flags') in the file path.
    :type file_type_index: int
    :return: dict of file paths.
    """
    empty_data_path = None
    empty_flags_path = None
    empty_uncertainty_data_path = None
    for file_path in crawl(empty_files_path):
        file_type = pathlib.Path(file_path).parts[file_type_index]
        if 'data' == file_type:
            empty_data_path = file_path
        elif 'flags' == file_type:
            empty_flags_path = file_path
        elif 'uncertainty_data' == file_type:
            empty_uncertainty_data_path = file_path
    return {'empty_data_path': empty_data_path,
            'empty_flags_path': empty_flags_path,
            'empty_uncertainty_data_path': empty_uncertainty_data_path}


def link_empty_file(target_dir, empty_file_path, file_name):
    """
    Link the empty file into the target path.

    :param target_dir: The target directory for writing files.
    :type target_dir: str
    :param empty_file_path: The source empty file path.
    :type empty_file_path: str
    :param file_name: The file name.
    :type file_name: str
    :return:
    """
    target_path = os.path.join(target_dir, file_name)
    # log.debug(f'target_path: {target_path}')
    link(empty_file_path, target_path)


def render_empty_file_name(file_name, location_name, year, month, day):
    """
    The empty file names contain generic 'location', 'year', 'month', and 'day'
    placeholders. Replace these strings with actual data.
    :param file_name: The empty file name.
    :type file_name: str
    :param location_name: The location name.
    :type location_name: str
    :param year: The file year.
    :type year: str
    :param month: The file month.
    :type month: str
    :param day: The file day.
    :type day: str
    :return:
    """
    file_name = file_name.replace('location', location_name)
    file_name = file_name.replace('year', year)
    file_name = file_name.replace('month', month)
    file_name = file_name.replace('day', day)
    return file_name
