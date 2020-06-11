#!/usr/bin/env python3
from pathlib import Path
from datetime import date


class DateGapFillerConfig(object):

    calibration_dir = 'calibration'
    data_dir = 'data'
    flag_dir = 'flags'
    location_dir = 'location'
    uncertainty_coefficient_dir = 'uncertainty_coef'
    uncertainty_data_dir = 'uncertainty_data'

    def __init__(self, *,
                 data_path: Path,
                 location_path: Path,
                 empty_file_path: Path,
                 out_path: Path,
                 start_date: date,
                 end_date: date,
                 output_directories: list,
                 empty_file_type_index):
        """
        Constructor.

        :param data_path: The path to read data files.
        :param location_path: The path to read location files.
        :param empty_file_path: The path to read empty files.
        :param out_path: The path to link files into.
        :param output_directories: The directory names to output.
        :param empty_file_type_index: The index of the file type in empty file paths.
        """
        self.data_path = data_path
        self.location_path = location_path
        self.empty_files_path = empty_file_path
        self.out_path = out_path
        self.start_date = start_date
        self.end_date = end_date
        self.output_directories = output_directories
        self.empty_file_type_index = empty_file_type_index
