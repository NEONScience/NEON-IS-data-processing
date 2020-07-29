#!/usr/bin/env python3
from pathlib import Path
from typing import List

import structlog

from data_path_order.data_path_order_config import Config

log = structlog.get_logger()


def order_paths(config: Config, indices: List[int]):
    for path in config.data_path.rglob('*'):
        if path.is_file():
            link_path = order_path(path, indices, config.out_path)
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                log.debug(f'path: {path} link_path: {link_path}')
                link_path.symlink_to(path)


def order_path(path: Path, indices: List[int], base_path: Path) -> Path:
    path_parts = path.parts
    new_path = Path(base_path)
    for index in indices:
        part = path_parts[index]
        new_path = new_path.joinpath(part)
    return new_path
