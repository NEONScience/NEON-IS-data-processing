#!/usr/bin/env python3
from pathlib import Path

import structlog

from data_calibration_group.calibration_file_path import CalibrationFilePath

log = structlog.get_logger()


class CalibrationFileLinker(object):

    def __init__(self, *, calibration_path: Path, calibration_file_path: CalibrationFilePath):
        """
        Constructor.

        :param calibration_path: The path to calibration files.
        :param calibration_file_path: The file path parser.
        """
        self.calibration_path = calibration_path
        self.calibration_file_path = calibration_file_path

    def link_files(self, source_id: str, output_path: Path):
        """
        Find calibrations for the source ID. If no files are found create an empty directory.

        :param source_id: The source ID of the data file to match on.
        :param output_path: The common output directory path for file grouping.
        """
        for path in self.calibration_path.rglob('*'):
            log.debug(f'calibration file path: {path}')
            parts = path.parts
            if path.is_dir():
                if len(parts) < self.calibration_file_path.stream_index:
                    Path(output_path, 'calibration').mkdir(parents=True, exist_ok=True)
            elif path.is_file():
                log.debug(f'path is file: {path}')
                source_type, calibration_source_id, stream = self.calibration_file_path.parse(path)
                log.debug(f'calibration type: {source_type}, id: {source_id}, stream: {stream}')
                if calibration_source_id == source_id:
                    link_path = Path(output_path, 'calibration', stream, path.name)
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    link_path.symlink_to(path)
