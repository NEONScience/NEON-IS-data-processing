#!/usr/bin/env python3
from pathlib import Path

import structlog

from common.data_filename import DataFilename

log = structlog.get_logger()


def link_data(data_path: Path,
              out_path: Path,
              source_type_index: int,
              year_index: int,
              month_index: int,
              day_index: int,
              file_index: int):
    """
    Link data files into the output path and yield the output directory.

    :param data_path: The path to the data files.
    :param out_path: The output path to write grouped files.
    :param source_type_index: The file path source type index.
    :param year_index: The file path year index.
    :param month_index: The file path month index.
    :param day_index: The file path day index.
    :param file_index: The file path file index.
    :return: Yields the output directory path for each data file.
    """
    for path in data_path.rglob('*'):
        if path.is_file():
            parts = path.parts
            source_type = parts[source_type_index]
            year = parts[year_index]
            month = parts[month_index]
            day = parts[day_index]
            filename = parts[file_index]
            source_id = DataFilename(filename).source_id()
            output_path = Path(out_path, source_type, year, month, day, source_id)
            link_path = Path(output_path, 'data', filename)
            log.debug(f'link path: {link_path}')
            link_path.parent.mkdir(parents=True, exist_ok=True)
            link_path.symlink_to(path)
            yield output_path


def link_location(location_path: Path, output_path: Path):
    """
    Link the location file.

    :param location_path: The path to the file.
    :param output_path: The output path.
    :return:
    """
    for path in location_path.rglob('*'):
        if path.is_file():
            link_path = Path(output_path, 'location', path.name)
            log.debug(f'location link path: {link_path}')
            link_path.parent.mkdir(parents=True, exist_ok=True)
            link_path.symlink_to(path)
