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
    data_path: Path = os.environ['DATA_PATH']
    map_path: Path = os.environ['MAP_PATH']
    out_path: Path = os.environ['OUT_PATH']
    log_level: str = os.environ['LOG_LEVEL']
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')
    print(f'final output path is: {Path(out_path)}')

    for dpath in Path("/pfs/DATA_PATH/").rglob('*'):
        if Path(dpath).is_file():
            print(f'you are inside file and the file name is : {dpath}')
            mac_address = Path(dpath).stem.split("_")[1]
            new_dpath = os.path.normpath(dpath)
            print(f'data path file name is: {new_dpath}')
            print(f'Mac_Address name is: {mac_address}')
            path_list = new_dpath.split(os.sep)
            for mpath in Path(map_path).rglob('*'):
                if Path(mpath).is_file():
                    assetuid = ''
                    if (Path(mpath).stem.split("_")[2] == mac_address):
                        with open(Path(mpath), 'r') as f:
                            asset_uid = f.read().split()[0]
                            print(f'Asset_UID name is: {asset_uid}')
                        f.close()
                        extended_path = os.sep.join(path_list[3:]).replace(str(mac_address), str(asset_uid))
                        print(f'Extended Path is: {extended_path}')
                        new_file_path = Path(os.path.join(Path(out_path), extended_path))
                        print(f'New file path after converting mac address to assetuid is: {new_file_path}')
                        Path(new_file_path).parent.mkdir(parents=True, exist_ok=True)
                        if not new_file_path.exists():
                            print(f'original path: {dpath} new_path: {new_file_path}')
                            log.debug(f'original path: {dpath} new_path: {new_file_path}')
                            new_file_path.symlink_to(new_dpath)


if __name__ == '__main__':
    load()
