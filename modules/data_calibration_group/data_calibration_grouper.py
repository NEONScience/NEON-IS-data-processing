#!/usr/bin/env python3
from pathlib import Path

import structlog

from data_calibration_group.data_path_parser import DataPathParser
from data_calibration_group.calibration_path_parser import CalibrationPathParser
from data_calibration_group.data_calibration_group_config import Config

log = structlog.get_logger()


def group_files(config: Config):
    parser = DataPathParser(config)
    for path in config.data_path.rglob('*'):
        if path.is_file():
            log.debug(f'data file path: {path}')
            source_id = path.name.split('_')[1]
            source_type, year, month, day = parser.parse(path)
            common_path = Path(config.out_path, source_type, year, month, day, source_id)
            link_data(common_path, path)
            link_calibrations(config, common_path, source_id)


def link_data(common_path, path):
    link_path = Path(common_path, 'data', path.name)
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        link_path.symlink_to(path)


def link_calibrations(config: Config, common_path: Path, source_id: str):
    parser = CalibrationPathParser(config)
    for path in config.calibration_path.rglob('*'):
        log.debug(f'calibration file path: {path}')
        if path.is_dir():
            if len(path.parts) < parser.stream_index:
                calibration_dir = Path(common_path, 'calibration')
                if not calibration_dir.exists():
                    calibration_dir.mkdir(parents=True, exist_ok=True)
        elif path.is_file():
            source_type, calibration_source_id, stream = parser.parse(path)
            if calibration_source_id == source_id:
                link_path = Path(common_path, 'calibration', stream, path.name)
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
