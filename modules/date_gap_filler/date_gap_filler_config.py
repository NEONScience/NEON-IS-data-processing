#!/usr/bin/env python3
from pathlib import Path


class DateGapFillerConfig(object):

    def __init__(self, *,
                 data_path: Path,
                 location_path: Path,
                 empty_file_path: Path,
                 out_path: Path,
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
        self.output_directories = output_directories
        self.empty_file_type_index = empty_file_type_index
        self.calibration_dir = 'calibration'
        self.data_dir = 'data'
        self.flags_dir = 'flags'
        self.location_dir = 'location'
        self.uncertainty_coefficient_dir = 'uncertainty_coef'
        self.uncertainty_data_dir = 'uncertainty_data'
