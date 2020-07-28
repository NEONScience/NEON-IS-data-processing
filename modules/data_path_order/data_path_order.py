#!/usr/bin/env python3
from pathlib import Path

import structlog

from data_path_order.data_path_parser import DataPathParser
from data_path_order.data_path_order_config import Config

log = structlog.get_logger()


def order_paths(config: Config):
    parser = DataPathParser(config)
    for path in config.data_path.rglob('*'):
        if path.is_file():
            source_type, year, month, day, source_id = parser.parse(path)
            link_path = Path(config.out_path, source_type, source_id, year, month, day, path.name)
            log.debug(f'path: {path} link_path: {link_path}')
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                link_path.symlink_to(path)
