#!usr/bin/env python3
import os
import logging
from pathlib import Path
from contextlib import closing
from structlog import get_logger
import os

log = get_logger()


def load_assetuid(data_path: Path, map_path: Path, out_path: Path, relative_path_index: int) -> None:

    mac_asset_map = {}
    for root, directories, files in os.walk(str(map_path)):
        if files:
            for file in files:
                mac = root.split(os.sep)[-1]
                with open(os.path.join(root, file), 'r') as f:
                    asset = f.read().split()[0]
                mac_asset_map[mac] = asset

    for root, directories, files in os.walk(str(data_path)):
        if files:
            for file in files:
                mac_address = file.split("_")[1]
                asset_uid = mac_asset_map.get(mac_address)
                log.debug(f'Mac_Address name is: {mac_address}; asset_uid is: {asset_uid}')

                old_path = os.path.join(root, file)
                path_list = old_path.split(os.sep)
                extended_path = os.sep.join(path_list[relative_path_index:]).replace(str(mac_address), str(asset_uid))
                new_path = Path(os.path.join(Path(out_path), extended_path))

                Path(new_path).parent.mkdir(parents=True, exist_ok=True)
                if not new_path.exists():
                    log.debug(f'original path: {file}; new_path: {new_path}')
                    new_path.symlink_to(old_path)




