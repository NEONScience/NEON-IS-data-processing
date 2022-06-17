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

    
   # for path in map_path.rglob('*'):
    for path in map_path:
        print(f'map path value is: {path}')
        if path.is_file():
            mac_address = os.path.basename(os.path.dirname(path))
            with open(path, 'r') as f:
                asset_uid = f.readlines()
        for dpath in data_path.rglob('*'):
            if dpath.is_file():
                    if (Path(dpath).stem.split("_")[1] == mac_address):
                        new_file_path = Path(out_path, dpath)
                        new_file_path.replace(mac_address, asset_uid)
                        new_file_path.parent.mkdir(parents=True, exist_ok=True)
                        if not new_file_path.exists():
                          log.debug(f'original path: {dpath} new_path: {new_file_path}')
                          new_file_path.symlink_to(new_file_path)
                          

if __name__ == '__main__':
    load()
