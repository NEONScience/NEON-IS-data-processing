#!/usr/bin/env python3
from pathlib import Path

from data_calibration_group.data_file_linker import DataFileLinker
from data_calibration_group.calibration_file_linker import CalibrationFileLinker


class DataCalibrationGrouper(object):
    """Class to group data and calibration files."""

    def __init__(self, *,
                 data_path: Path,
                 calibration_path: Path,
                 out_path: Path,
                 data_source_type_index: int,
                 data_year_index: int,
                 data_month_index: int,
                 data_day_index: int,
                 calibration_source_type_index: int,
                 calibration_source_id_index: int,
                 calibration_stream_index: int):

        self.data_file_linker = DataFileLinker(data_path=data_path,
                                               out_path=out_path,
                                               source_type_index=data_source_type_index,
                                               year_index=data_year_index,
                                               month_index=data_month_index,
                                               day_index=data_day_index)

        self.calibration_file_linker = CalibrationFileLinker(calibration_path=calibration_path,
                                                             source_type_index=calibration_source_type_index,
                                                             source_id_index=calibration_source_id_index,
                                                             stream_index=calibration_stream_index)

    def group(self):
        for source in self.data_file_linker.link_files():
            source_id = source.get('source_id')
            output_path = source.get('output_path')
            self.calibration_file_linker.link_files(source_id, output_path)
