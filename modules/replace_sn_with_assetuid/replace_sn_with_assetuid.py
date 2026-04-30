#!usr/bin/env python3
import os
from pathlib import Path
from structlog import get_logger

log = get_logger()


def load_assetuid(data_path: Path, map_path: Path, out_path: Path, relative_path_index: int) -> None:

    sn_asset_map = {}
    for path in map_path.rglob('*'):
        log.debug(f'map_path: {path}')
        if path.is_file():
            sn = path.parent.name
            log.debug(f'sn: {sn}')
            with open(path, 'r') as f:
                asset = f.read().split()[0]
                log.debug(f'asset: {asset}')
            sn_asset_map[sn.upper()] = asset

    for path in data_path.rglob('*'):
        if path.is_file():
            log.debug(f'data_path: {path}')
            serial_number = path.parent.parent.name
            serial_number = serial_number.upper()
            asset_uid = sn_asset_map.get(serial_number)
            log.debug(f'serial_number is: {serial_number}; asset_uid is: {asset_uid}')

            new_path = Path(out_path, os.sep.join(path.parts[relative_path_index:]).replace(serial_number, asset_uid))

            Path(new_path).parent.mkdir(parents=True, exist_ok=True)
            if not new_path.exists():
                log.debug(f'original path: {path}; new_path: {new_path}')
                new_path.symlink_to(path)




