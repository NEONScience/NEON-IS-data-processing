#!/usr/bin/env python3
import os
import sys
import pathlib
import datetime

import argparse
import environs
from structlog import get_logger

import lib.file_linker as file_linker
import lib.log_config as log_config
import lib.file_crawler as file_crawler

import timeseries_padder.timeseries_padder.padder_util as padder_util
import timeseries_padder.timeseries_padder.output_writer as output_writer


log = get_logger()


class ConstantPadder(object):

    def __init__(self, data_path, out_path, year_index, month_index, day_index,
                 location_index, sub_dir_index, window_size):
        """
        Constructor.

        :param data_path: The data path.
        :type data_path: str
        :param out_path:  The path for writing results.
        :type out_path: str
        :param year_index: The year index in the file path.
        :type year_index: int
        :param month_index: The month index in the file path.
        :type month_index: int
        :param day_index: The day index in the file path.
        :type day_index: int
        :param location_index: The location index in the file path.
        :type location_index: int
        :param sub_dir_index: The sub directory index in the file path.
        :type sub_dir_index: int
        :param window_size: The window size.
        :type window_size: int
        """
        self.data_path = data_path
        self.out_path = out_path
        # date and location indices in file path
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.location_index = location_index
        self.sub_dir_index = sub_dir_index
        self.window_size = window_size
        self.sub_dirs_to_process = ['data']
        self.out_dir_parts = list(pathlib.Path(out_path).parts)

    def pad(self):
        """
        Pad the data for the window size.

        :return:
        """
        try:
            manifests = {}
            manifest_file_names = {}
            for file_path in file_crawler.crawl(self.data_path):
                parts = pathlib.Path(file_path).parts
                year = parts[self.year_index]
                month = parts[self.month_index]
                day = parts[self.day_index]
                location = parts[self.location_index]
                sub_dir = parts[self.sub_dir_index]
                if sub_dir in self.sub_dirs_to_process:
                    location_path = os.path.join(*parts[0:self.location_index + 1])
                    if location not in manifests:
                        manifests[location] = []
                    # get data date
                    date = datetime.date(int(year), int(month), int(day))
                    # get dates in padded range
                    dates_in_padded_range = padder_util.get_dates_in_padded_range(date, self.window_size)
                    # link file into each date in padded range
                    destination_parts = list(parts)
                    for index in range(1, len(self.out_dir_parts)):
                        destination_parts[index] = self.out_dir_parts[index]
                    for date_in_padded_range in dates_in_padded_range:
                        destination_parts[self.year_index] = str(date_in_padded_range.year)
                        destination_parts[self.month_index] = str(date_in_padded_range.month).zfill(2)
                        destination_parts[self.day_index] = str(date_in_padded_range.day).zfill(2)
                        # generate destination path
                        destination_path = os.path.join(*destination_parts)
                        log.debug(f'source: {file_path}')
                        log.debug(f'destination: {destination_path}')
                        file_linker.link(file_path, destination_path)
                        manifests[location].append(date_in_padded_range)
                        if date_in_padded_range == date:
                            # construct manifest filename
                            manifest_path = os.path.dirname(destination_path)  # remove data filename
                            manifest_file_names[location] = os.path.join(manifest_path, 'manifest.txt')
                        output_writer.write_thresholds(location_path, destination_path)
                else:
                    destination_path = os.path.join(self.out_path, *parts[3:])
                    file_linker.link(file_path, destination_path)
            output_writer.write_manifests(manifests, manifest_file_names)  # write manifest files
        except Exception:
            exception_type, exception_obj, exception_tb = sys.exc_info()
            log.error("Exception at line " + str(exception_tb.tb_lineno) + ": " + str(sys.exc_info()))


def main():
    env = environs.Env()
    data_path = env('DATA_PATH')
    out_path = env('OUT_PATH')
    log_level = env('LOG_LEVEL')
    window_size = env('WINDOW_SIZE')
    log_config.configure(log_level)
    log.debug(f'data_dir: {data_path}')
    log.debug(f'out_dir: {out_path}')
    parser = argparse.ArgumentParser()
    parser.add_argument('--yearindex')
    parser.add_argument('--monthindex')
    parser.add_argument('--dayindex')
    parser.add_argument('--locindex')
    parser.add_argument('--subdirindex')
    args = parser.parse_args()
    cp = ConstantPadder(data_path, out_path, int(args.yearindex), int(args.monthindex),
                        int(args.dayindex), int(args.locindex), int(args.subdirindex), int(window_size))
    cp.pad()


if __name__ == '__main__':
    main()
