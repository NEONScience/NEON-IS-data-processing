#!/usr/bin/env python3
from pathlib import Path

import structlog

from data_calibration_group.data_path_parser import DataPathParser
from data_calibration_group.calibration_path_parser import CalibrationPathParser
from data_calibration_group.data_calibration_group_config import Config

log = structlog.get_logger()


def group_files(config: Config) -> None:
    parser = DataPathParser(config)
    for path in config.data_path.rglob('*'):
        if path.is_file():
            log.debug(f'data file path: {path}')
            source_type, source_id, year, month, day = parser.parse(path)
            common_path = Path(config.out_path, source_type, year, month, day, source_id)
            link_calibrations(config, common_path, source_id)


def link_calibrations(config: Config, common_path: Path, source_id: str) -> None:
    parser = CalibrationPathParser(config)
    for path in config.calibration_path.rglob('*'):
        log.debug(f'calibration file path: {path}')
        if path.is_file():
            source_type, calibration_source_id, stream = parser.parse(path)
            if calibration_source_id == source_id:
                link_path = Path(common_path, 'calibration', stream, path.name)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
