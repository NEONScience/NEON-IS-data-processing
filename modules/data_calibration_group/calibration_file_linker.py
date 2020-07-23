#!/usr/bin/env python3
from pathlib import Path

import structlog

from data_calibration_group.data_calibration_group_config import Config
from data_calibration_group.calibration_path_parser import CalibrationPathParser

log = structlog.get_logger()


class CalibrationFileLinker:

    def __init__(self, config: Config):
        self.calibration_path = config.calibration_path
        self.stream_index = config.calibration_stream_index
        self.path_parser = CalibrationPathParser(config)

    def link_files(self, source_id: str, output_path: Path):
        """
        Find calibrations for the source ID. If no files are found create an empty directory.

        :param source_id: The source ID of the data file to match on.
        :param output_path: The common output directory path for file grouping.
        """
        for path in self.calibration_path.rglob('*'):
            log.debug(f'calibration file path: {path}')
            if path.is_dir():
                if len(path.parts) < self.stream_index:
                    Path(output_path, 'calibration').mkdir(parents=True, exist_ok=True)
            elif path.is_file():
                log.debug(f'path is file: {path}')
                source_type, calibration_source_id, stream = self.path_parser.parse(path)
                log.debug(f'calibration type: {source_type}, id: {source_id}, stream: {stream}')
                if calibration_source_id == source_id:
                    link_path = Path(output_path, 'calibration', stream, path.name)
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        link_path.symlink_to(path)
