#!usr/bin/env python3
import os
from pathlib import Path
from structlog import get_logger

log = get_logger()


def load_assetuid(data_path: Path, map_path: Path, out_path: Path, relative_path_index: int) -> None:

    mac_asset_map = {}
    for path in map_path.rglob('*'):
        if path.is_file():
            mac = path.parent.name
            with open(path, 'r') as f:
                asset = f.read().split()[0]
            mac_asset_map[mac] = asset

    for path in data_path.rglob('*'):
        if path.is_file():
            mac_address = path.parent.parent.name
            asset_uid = mac_asset_map.get(mac_address)
            log.debug(f'Mac_Address name is: {mac_address}; asset_uid is: {asset_uid}')

            new_path = Path(out_path, os.sep.join(path.parts[relative_path_index:]).replace(mac_address, asset_uid))

            Path(new_path).parent.mkdir(parents=True, exist_ok=True)
            if not new_path.exists():
                log.debug(f'original path: {path}; new_path: {new_path}')
                new_path.symlink_to(path)




