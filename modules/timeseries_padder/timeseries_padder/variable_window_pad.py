#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import datetime

from structlog import get_logger

import timeseries_padder.timeseries_padder.pad_calculator as pad_calculator
import timeseries_padder.timeseries_padder.file_writer as file_writer
from timeseries_padder.timeseries_padder.pad_config import PadConfig
from timeseries_padder.timeseries_padder.data_file_path import DataFilePath


log = get_logger()


class VariableWindowPad(object):

    def __init__(self, data_path: Path, out_path: Path, data_file_path: DataFilePath):
        """
        Constructor.

        :param data_path: The data path.
        :param out_path: The output path for writing results.
        :param data_file_path: The data file path parser
        """
        self.data_path = data_path
        self.out_path = out_path
        self.data_file_path = data_file_path
        self.data_types = [PadConfig.data_dir]
        self.out_dir_parts = list(out_path.parts)

    def pad(self):
        """Pad the data to the calculated window size."""
        try:
            max_window_size_by_date_and_location = {}
            min_data_rate_by_date_and_location = {}
            manifests = {}
            manifest_file_names = {}
            for path in self.data_path.rglob('*'):
                if path.is_file():
                    parts = path.parts
                    year, month, day, location, data_type = self.data_file_path.parse(path)
                    if data_type in self.data_types:
                        date_location_key = year+month+day+location
                        config_location_path = Path(*parts[:self.data_file_path.location_index + 1])
                        if location not in manifests:
                            manifests[location] = []
                        # get min of all data rates (to ensure adequate window coverage)
                        if date_location_key not in min_data_rate_by_date_and_location:
                            location_path = Path(config_location_path, PadConfig.location_dir)
                            location_files = [f for f in os.listdir(location_path)
                                              if f.endswith(PadConfig.location_file_extension)]
                            location_file = Path(location_path, location_files[0])
                            min_data_rate_by_date_and_location[date_location_key] = \
                                pad_calculator.get_min_data_rate(location_file)
                        data_rate = min_data_rate_by_date_and_location[date_location_key]
                        # get max of all window sizes
                        if date_location_key not in max_window_size_by_date_and_location:
                            threshold_path = Path(config_location_path, PadConfig.threshold_dir)
                            threshold_files = [f for f in os.listdir(threshold_path)
                                               if f.endswith(PadConfig.threshold_file_extension)]
                            threshold_file = Path(threshold_path, threshold_files[0])
                            log.debug(f'threshold file: {threshold_file}')
                            max_window_size_by_date_and_location[date_location_key] = \
                                pad_calculator.get_max_window_size(threshold_file, data_rate)
                        window_size = max_window_size_by_date_and_location[date_location_key]
                        data_date = datetime.date(int(year), int(month), int(day))
                        # calculate pad size
                        pad_size = pad_calculator.calculate_pad_size(window_size)
                        padded_range_dates = pad_calculator.get_dates_in_padded_range(data_date, pad_size)
                        # link file into each date in the padded range
                        link_parts = list(parts)
                        for index in range(1, len(self.out_dir_parts)):
                            link_parts[index] = self.out_dir_parts[index]
                        for padded_range_date in padded_range_dates:
                            link_parts[self.data_file_path.year_index] = str(padded_range_date.year)
                            link_parts[self.data_file_path.month_index] = str(padded_range_date.month).zfill(2)
                            link_parts[self.data_file_path.day_index] = str(padded_range_date.day).zfill(2)
                            link_path = Path(*link_parts)
                            log.debug(f'file: {path} link: {link_path}')
                            link_path.parent.mkdir(parents=True, exist_ok=True)
                            if not link_path.exists():
                                link_path.symlink_to(path)
                            manifests[location].append(padded_range_date)
                            if padded_range_date == data_date:
                                manifest_path = link_path.parent
                                manifest_file_names[location] = os.path.join(manifest_path, PadConfig.manifest_filename)
                            file_writer.link_thresholds(config_location_path, link_path)
            file_writer.write_manifests(manifests, manifest_file_names)
        except Exception:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            log.error("Exception at line " + str(exc_tb.tb_lineno) + ": " + str(sys.exc_info()))
