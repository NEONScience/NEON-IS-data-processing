#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import datetime

from structlog import get_logger

from common.file_linker import link
import timeseries_padder.timeseries_padder.padder_util as padder_util
import timeseries_padder.timeseries_padder.file_writer as file_writer


log = get_logger()


class Padder(object):

    def __init__(self,
                 data_path: Path,
                 out_path: Path,
                 year_index: int,
                 month_index: int,
                 day_index: int,
                 location_index: int,
                 subdir_index: int):
        """
        Constructor.

        :param data_path: The data path.
        :param out_path: The output path for writing results.
        :param year_index: The year index in the file path.
        :param month_index: The month index in the file path.
        :param day_index: The day index in the file path.
        :param location_index: The location index in the file path.
        :param subdir_index: The sub directory index in the file path.
        """
        self.data_path = data_path
        self.out_path = out_path
        # date and location indices in file path
        self.year_index = year_index
        self.month_index = month_index
        self.day_index = day_index
        self.config_location_index = location_index
        self.sub_dir_index = subdir_index
        self.sub_dirs_to_output = ['data']
        self.out_dir_parts = list(out_path.parts)

    def pad(self):
        """Pad the data to the calculated window size."""
        try:
            max_window_size_by_date_and_location = {}
            min_data_rate_by_date_and_location = {}
            manifests = {}
            manifest_file_names = {}
            for root, dirs, files in os.walk(str(self.data_path)):
                for filename in files:
                    file_path = Path(root, filename)
                    parts = file_path.parts
                    subdir = parts[self.sub_dir_index]
                    if subdir in self.sub_dirs_to_output:
                        year = parts[self.year_index]
                        month = parts[self.month_index]
                        day = parts[self.day_index]
                        config_location = parts[self.config_location_index]
                        date_location_key = year+month+day+config_location

                        config_location_path = Path(*parts[0:self.config_location_index + 1])

                        if config_location not in manifests:
                            manifests[config_location] = []

                        # get min of all data rates (to ensure adequate window coverage)
                        if date_location_key not in min_data_rate_by_date_and_location:
                            location_path = Path(config_location_path, 'location')
                            location_files = [f for f in os.listdir(location_path) if f.endswith('.json')]
                            location_file = Path(location_path, location_files[0])
                            min_data_rate_by_date_and_location[date_location_key] = \
                                padder_util.get_min_data_rate(location_file)
                        data_rate = min_data_rate_by_date_and_location[date_location_key]

                        # get max of all window sizes
                        if date_location_key not in max_window_size_by_date_and_location:
                            threshold_path = Path(config_location_path, 'threshold')
                            threshold_files = [f for f in os.listdir(threshold_path) if f.endswith('.json')]
                            threshold_file = Path(threshold_path, threshold_files[0])
                            log.debug(f'thresholdFile: {threshold_file}')
                            max_window_size_by_date_and_location[date_location_key] = \
                                padder_util.get_max_window_size(threshold_file, data_rate)
                        window_size = max_window_size_by_date_and_location[date_location_key]

                        # get data date
                        date = datetime.date(int(year), int(month), int(day))
                        # calculate pad size
                        pad_size = padder_util.calculate_pad_size(window_size)
                        # get dates in padded range
                        dates_in_padded_range = padder_util.get_dates_in_padded_range(date, pad_size)
                        # link file into each date in padded range
                        link_parts = list(parts)
                        for index in range(1, len(self.out_dir_parts)):
                            link_parts[index] = self.out_dir_parts[index]
                        for dateInPaddedRange in dates_in_padded_range:
                            link_parts[self.year_index] = str(dateInPaddedRange.year)
                            link_parts[self.month_index] = str(dateInPaddedRange.month).zfill(2)
                            link_parts[self.day_index] = str(dateInPaddedRange.day).zfill(2)
                            link_path = Path(*link_parts)
                            log.debug(f'file: {file_path} link: {link_path}')
                            link(file_path, link_path)
                            manifests[config_location].append(dateInPaddedRange)
                            if dateInPaddedRange == date:
                                manifest_path = link_path.parent
                                manifest_file_names[config_location] = os.path.join(manifest_path, 'manifest.txt')
                            file_writer.link_thresholds(config_location_path, link_path)
            file_writer.write_manifests(manifests, manifest_file_names)

        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.error("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))
