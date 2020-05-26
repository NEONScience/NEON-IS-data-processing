#!/usr/bin/env python3
from pathlib import Path

import structlog

log = structlog.get_logger()


class CalibrationGrouper(object):

    def __init__(self,
                 calibration_path: Path,
                 calibration_source_type_index: int,
                 calibration_source_id_index: int,
                 calibration_stream_index: int):
        """
        Constructor.

        :param calibration_path: The path to calibration files.
        :param calibration_source_type_index: The source type index in the calibration path.
        :param calibration_source_id_index: The source ID index in the calibration path.
        :param calibration_stream_index: The stream index in the calibration path.
        """
        self.calibration_path = calibration_path
        self.calibration_source_type_index = calibration_source_type_index
        self.calibration_source_id_index = calibration_source_id_index
        self.calibration_stream_index = calibration_stream_index

    def link_calibrations(self, source_id: str, output_path: Path):
        """
        Find calibrations for the source ID. If no files are found create an empty directory.

        :param source_id: The source ID of the data file to match on.
        :param output_path: The common output directory path for file grouping.
        :return:
        """
        for path in self.calibration_path.rglob('*'):
            log.debug(f'calibration file path: {path}')
            parts = path.parts
            if path.is_dir():
                if len(parts) < self.calibration_stream_index:
                    Path(output_path, 'calibration').mkdir(parents=True, exist_ok=True)
            elif path.is_file():
                log.debug(f'path is file: {path}')
                source_type = parts[self.calibration_source_type_index]
                calibration_source_id = parts[self.calibration_source_id_index]
                stream = parts[self.calibration_stream_index]
                log.debug(f'calibration type: {source_type}, id: {source_id}, stream: {stream}')
                if calibration_source_id == source_id:
                    link_path = Path(output_path, 'calibration', stream, path.name)
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    link_path.symlink_to(path)
