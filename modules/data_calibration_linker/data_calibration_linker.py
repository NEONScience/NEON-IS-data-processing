#!/usr/bin/env python3
from pathlib import Path

import structlog

from data_calibration_linker.data_path_parser import DataPathParser
from data_calibration_linker.data_calibration_config import Config

log = structlog.get_logger()


def process_files(config: Config):
    if config.data_path is not None:
        log.debug('linking data')
        link_data(config)
    if config.calibration_path is not None:
        log.debug('linking calibrations')
        link_calibrations(config)


def link_data(config: Config):
    parser = DataPathParser(config)
    for path in config.data_path.rglob('*'):
        if path.is_file():
            log.debug(f'data file path: {path}')
            source_type, source_id, year, month, day = parser.parse(path)
            common_path = Path(config.out_path, source_type, year, month, day, source_id)
            link_path = Path(common_path, 'data', path.name)
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                log.debug(f'data_link: {link_path}')
                link_path.symlink_to(path)
            # create an empty calibration directory for each data file
            log.debug('creating empty calibration directory')
            calibration_path = Path(common_path, 'calibration')
            calibration_path.mkdir(parents=True, exist_ok=True)


def link_calibrations(config: Config):
    for path in config.calibration_path.rglob('*'):
        if path.is_file():
            parts = path.parts
            link_path = Path(config.out_path, *parts[config.relative_path_index:])
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                log.debug(f'calibration_link: {link_path}')
                link_path.symlink_to(path)

