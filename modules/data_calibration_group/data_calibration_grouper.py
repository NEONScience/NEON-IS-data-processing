#!/usr/bin/env python3
from pathlib import Path

from data_calibration_group.data_file_path import DataFilePath
from data_calibration_group.data_file_linker import DataFileLinker
from data_calibration_group.calibration_file_path import CalibrationFilePath
from data_calibration_group.calibration_file_linker import CalibrationFileLinker


class DataCalibrationGrouper(object):
    """Class to group data and calibration files."""

    def __init__(self, *,
                 data_path: Path,
                 calibration_path: Path,
                 out_path: Path,
                 data_file_path: DataFilePath,
                 calibration_file_path: CalibrationFilePath):

        self.data_file_linker = DataFileLinker(data_path=data_path,
                                               out_path=out_path,
                                               data_file_path=data_file_path)

        self.calibration_file_linker = CalibrationFileLinker(calibration_path=calibration_path,
                                                             calibration_file_path=calibration_file_path)

    def group_files(self):
        for source in self.data_file_linker.link_files():
            source_id = source.get('source_id')
            output_path = source.get('output_path')
            self.calibration_file_linker.link_files(source_id, output_path)
