#!/usr/bin/env python3
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig


class EmptyFiles(object):
    """Class to store empty file paths."""

    def __init__(self, config: DateGapFillerConfig):
        empty_files_path = config.empty_files_path
        empty_file_type_index = config.empty_file_type_index
        calibration_dir = config.calibration_dir
        data_dir = config.data_dir
        flags_dir = config.flags_dir
        uncertainty_coefficient_dir = config.uncertainty_coefficient_dir
        uncertainty_data_dir = config.uncertainty_data_dir
        self.calibration_path = None
        self.data_path = None
        self.flags_path = None
        self.uncertainty_coefficient_path = None
        self.uncertainty_data_path = None
        for path in empty_files_path.rglob('*'):
            if path.is_file():
                file_type_dir = path.parts[empty_file_type_index]
                if calibration_dir == file_type_dir:
                    self.calibration_path = path
                elif data_dir == file_type_dir:
                    self.data_path = path
                elif flags_dir == file_type_dir:
                    self.flags_path = path
                elif uncertainty_coefficient_dir == file_type_dir:
                    self.uncertainty_coefficient_path = path
                elif uncertainty_data_dir == file_type_dir:
                    self.uncertainty_data_path = path
