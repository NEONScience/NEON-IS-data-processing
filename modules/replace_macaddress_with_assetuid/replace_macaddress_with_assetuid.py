#!usr/bin/env python3
import os
import logging
from pathlib import Path
from contextlib import closing
from structlog import get_logger
import environs
import requests
import json
import argparse
import common.log_config as log_config
import os

log = get_logger()


def load() -> None:
    env = environs.Env
    data_path: Path = os.environ['DATA_PATH']
    map_path: Path = os.environ['MAP_PATH']
    out_path: Path = os.environ['OUT_PATH']
    log_level: str = os.environ['LOG_LEVEL']
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')
    print(f'final output path is: {Path(out_path)}')
    for path in Path(map_path).rglob('*'):
        path = Path(path)
        if path.is_file():
            print(f'map path file name is: {path}')
            mac_address = os.path.basename(os.path.dirname(path))
            print(f'Mac_Address name is: {mac_address}')
            with open(path, 'r') as f:
                asset_uid = f.read().split()[0]
                print(f'Asset_UID name is: {asset_uid}')
            for dpath in Path(data_path).rglob('*'):
                if dpath.is_file():
                    new_dpath = os.path.normpath(dpath)
                    print(f'data path file name is: {new_dpath}')
                    path_list = new_dpath.split(os.sep)
                    
                    if (dpath.stem.split("_")[1] == mac_address):
                        new_file_path = os.path.join(Path(out_path), os.sep.join(path_list[3:]))
                        print(f'Final output path is {new_file_path}')
                        str(new_file_path).replace(str(mac_address), str(asset_uid))
                        print(f'New file path after converting mac address to assetuid is: {new_file_path}')
                        new_file_path.parent.mkdir(parents=True, exist_ok=True)
                        if not new_file_path.exists():
                            print(f'original path: {dpath} new_path: {new_file_path}')
                            log.debug(f'original path: {dpath} new_path: {new_file_path}')
                            new_file_path.symlink_to(new_file_path)


if __name__ == '__main__':
    load()
