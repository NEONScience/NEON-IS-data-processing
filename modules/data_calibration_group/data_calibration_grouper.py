#!/usr/bin/env python3
from data_calibration_group.data_calibration_group_config import Config
from data_calibration_group.data_file_linker import DataFileLinker
from data_calibration_group.calibration_file_linker import CalibrationFileLinker


def group_files(config: Config):
    calibration_file_linker = CalibrationFileLinker(config)
    data_file_linker = DataFileLinker(config)
    for source_path in data_file_linker.link_files():
        source_id = source_path.source_id
        output_path = source_path.output_path
        calibration_file_linker.link_files(source_id, output_path)
