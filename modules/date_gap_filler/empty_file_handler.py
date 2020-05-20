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
    paths = {}
    for file_path in crawl(empty_files_path):
        file_type = pathlib.Path(file_path).parts[file_type_index]
        if 'data' == file_type:
            paths.update(data_path=file_path)
        elif 'flags' == file_type:
            paths.update(flags_path=file_path)
        elif 'uncertainty_data' == file_type:
            paths.update(uncertainty_path=file_path)
    return paths


def link_empty_file(output_dir, file, location, year, month, day):
    """
    Link the file into the output directory.

    :param output_dir: The target directory for linking files.
    :type output_dir: str
    :param file: The source empty file path.
    :type file: str
    :param location: The location.
    :type location: str
    :param year: The year.
    :type year: str
    :param month: The month.
    :type month: str
    :param day: The day.
    :type day: str
    :return:
    """
    filename = pathlib.Path(file).name
    filename = filename.replace('location', location).replace('year', year).replace('month', month).replace('day', day)
    filename += '.empty'  # add extension to distinguish from real data files.
    link_path = os.path.join(output_dir, filename)
    log.debug(f'source: {file}, link: {link_path}')
    link(file, link_path)
