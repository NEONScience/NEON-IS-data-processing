#!/usr/bin/env python3
import os
import pathlib

import structlog

from lib.file_linker import link
from lib.file_crawler import crawl

log = structlog.get_logger()


class CalibrationGrouper(object):

    def __init__(self,
                 calibration_path,
                 calibration_source_type_index,
                 calibration_source_id_index,
                 calibration_stream_index):
        """
        Constructor.
        :param calibration_path: The path to calibration files.
        :type calibration_path: str
        :param calibration_source_type_index: The source type index in the calibration path.
        :type calibration_source_type_index: int
        :param calibration_source_id_index: The source ID index in the calibration path.
        :type calibration_source_id_index: int
        :param calibration_stream_index: The stream index in the calibration path.
        :type calibration_stream_index: int
        """
        self.calibration_path = calibration_path
        self.calibration_source_type_index = calibration_source_type_index
        self.calibration_source_id_index = calibration_source_id_index
        self.calibration_stream_index = calibration_stream_index

    def link_calibrations(self, source_id, output_path):
        """
        Find calibrations for the source ID. If no files are found create an empty directory.

        :param source_id: The source ID of the data file to match on.
        :type source_id: str
        :param output_path: The common output directory path for file grouping.
        :type output_path: str
        :return:
        """
        for file_path in crawl(self.calibration_path):
            log.debug(f'calibration file path: {file_path}')
            parts = file_path.parts
            if len(parts) < self.calibration_stream_index:
                calibration_path = pathlib.Path(output_path, 'calibration')
                if not calibration_path.exists():
                    os.makedirs(calibration_path)  # create an empty calibration directory
            else:
                source_type = parts[self.calibration_source_type_index]
                calibration_source_id = parts[self.calibration_source_id_index]
                stream = parts[self.calibration_stream_index]
                log.debug(f'calibration type: {source_type}, id: {source_id}, stream: {stream}')
                if calibration_source_id == source_id:
                    calibration_dir = pathlib.Path(output_path, 'calibration', stream)
                    target_path = pathlib.Path(calibration_dir, file_path.name)
                    link(file_path, target_path)
