#!/usr/bin/env python3
import sys
from pathlib import Path
import datetime

import environs
from structlog import get_logger

from lib.file_linker import link
from lib.file_crawler import crawl
import lib.log_config as log_config

import timeseries_padder.timeseries_padder.padder_util as padder_util
import timeseries_padder.timeseries_padder.file_writer as file_writer

log = get_logger()


class ConstantPadder(object):

    def __init__(self,
                 data_path: Path,
                 out_path: Path,
                 year_index: int,
                 month_index: int,
                 day_index: int,
                 location_index: int,
                 data_type_index: int,
                 relative_path_index: int,
                 window_size: int):
        """
        Constructor.

        :param data_path: The data path.
        :param out_path:  The path for writing results.
        :param year_index: The year index in the file path.
        :param month_index: The month index in the file path.
        :param day_index: The day index in the file path.
        :param location_index: The location index in the file path.
        :param data_type_index: The sub directory index in the file path.
        :param relative_path_index: Trim file paths to this index.
        :param window_size: The window size.
        """
        self.data_path = data_path
        self.out_path = out_path
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.location_index = location_index
        self.sub_dir_index = data_type_index
        self.relative_path_index = relative_path_index
        self.window_size = window_size
        self.sub_dirs_to_process = ['data']
        self.out_path_parts = list(out_path.parts)

    def pad(self):
        """Pad the data for the window size."""
        try:
            manifests = {}
            manifest_file_names = {}
            for file_path in crawl(self.data_path):
                parts = file_path.parts
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                location = parts[self.location_index]
                sub_dir = parts[self.sub_dir_index]
                if sub_dir in self.sub_dirs_to_process:
                    location_path = Path(*parts[0:self.location_index + 1])
                    if location not in manifests:
                        manifests[location] = []
                    # get data date
                    date = datetime.date(int(year), int(month), int(day))
                    # get dates in padded range
                    dates_in_padded_range = padder_util.get_dates_in_padded_range(date, self.window_size)
                    # link file into each date in padded range
                    link_parts = list(parts)
                    for index in range(1, len(self.out_path_parts)):
                        link_parts[index] = self.out_path_parts[index]
                    for date_in_padded_range in dates_in_padded_range:
                        link_parts[self.year_index] = str(date_in_padded_range.year)
                        link_parts[self.month_index] = str(date_in_padded_range.month).zfill(2)
                        link_parts[self.day_index] = str(date_in_padded_range.day).zfill(2)
                        link_path = Path(*link_parts)
                        log.debug(f'file: {file_path} link: {link_path}')
                        link(file_path, link_path)
                        manifests[location].append(date_in_padded_range)
                        if date_in_padded_range == date:
                            manifest_path = link_path.parent  # remove data filename
                            manifest_file_names[location] = Path(manifest_path, 'manifest.txt')
                        file_writer.link_thresholds(location_path, link_path)
                else:
                    link_path = Path(self.out_path, *parts[self.relative_path_index:])
                    link(file_path, link_path)
            file_writer.write_manifests(manifests, manifest_file_names)  # write manifest files
        except Exception:
            exception_type, exception_obj, exception_tb = sys.exc_info()
            log.error("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))


def main():
    env = environs.Env()
    data_path = env.path('DATA_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL')
    window_size = env.int('WINDOW_SIZE')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    location_index = env.int('LOCATION_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)
    cp = ConstantPadder(data_path, out_path, year_index, month_index,
                        day_index, location_index, data_type_index,
                        relative_path_index, window_size)
    cp.pad()


if __name__ == '__main__':
    main()
