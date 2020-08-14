#!/usr/bin/env python3
from pathlib import Path

from date_gap_filler.empty_file_linker import EmptyFileLinker
from date_gap_filler.date_gap_filler_config import DateGapFillerConfig


class EmptyFileHandler(object):

    def __init__(self, config: DateGapFillerConfig) -> None:
        self.output_dirs = config.output_directories
        self.calibration_dir = config.calibration_dir
        self.data_dir = config.data_dir
        self.flags_dir = config.flag_dir
        self.uncertainty_coefficient_dir = config.uncertainty_coefficient_dir
        self.uncertainty_data_dir = config.uncertainty_data_dir

        self.link_calibration = True if self.calibration_dir in self.output_dirs else False
        self.link_data = True if self.data_dir in self.output_dirs else False
        self.link_flags = True if self.flags_dir in self.output_dirs else False
        self.link_uncertainty_coefficient = True if self.uncertainty_coefficient_dir in self.output_dirs else False
        self.link_uncertainty_data = True if self.uncertainty_data_dir in self.output_dirs else False

    def link_files(self, root_link_path: Path, empty_file_linker: EmptyFileLinker) -> None:
        if self.link_calibration:
            Path(root_link_path, self.calibration_dir).mkdir(parents=True, exist_ok=True)
        if self.link_data:
            empty_file_linker.link_data_file(Path(root_link_path, self.data_dir))
        if self.link_flags:
            empty_file_linker.link_flags_file(Path(root_link_path, self.flags_dir))
        if self.link_uncertainty_coefficient:
            Path(root_link_path, self.uncertainty_coefficient_dir).mkdir(parents=True, exist_ok=True)
        if self.link_uncertainty_data:
            empty_file_linker.link_uncertainty_data_file(Path(root_link_path, self.uncertainty_data_dir))
