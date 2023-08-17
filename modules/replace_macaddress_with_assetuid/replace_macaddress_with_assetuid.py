#!usr/bin/env python3
import os
import logging
from pathlib import Path
from contextlib import closing
from structlog import get_logger
import os

log = get_logger()


def load_assetuid(data_path: Path, map_path: Path, out_path: Path) -> None:

    for dpath in Path(data_path).rglob('*'):
        if Path(dpath).is_file():
            print(f'you are inside file and the file name is : {dpath}')
            mac_address = Path(dpath).stem.split("_")[1]
            new_dpath = os.path.normpath(dpath)
            print(f'data path file name is: {new_dpath}')
            print(f'Mac_Address name is: {mac_address}')
            print(f'new_dpath is: {new_dpath}')
            path_list = new_dpath.split(os.sep)
            print(f'path_list is: {path_list}')
            for mpath in Path(map_path).rglob('*'):
                print('mpath0 = ', Path(mpath).stem.split("_")[0])
                if Path(mpath).is_file():
                    asset_uid = ''
                    if (Path(mpath).stem.split("_")[2] == mac_address):
                        with open(Path(mpath), 'r') as f:
                            asset_uid = f.read().split()[0]
                            print(f'Asset_UID name is: {asset_uid}')
                        f.close()
                        extended_path = os.sep.join(path_list[2:]).replace(str(mac_address), str(asset_uid))
                        print(f'Extended Path is: {extended_path}')
                        new_file_path = Path(os.path.join(Path(out_path), extended_path))
                        Path(new_file_path).parent.mkdir(parents=True, exist_ok=True)
                        if not new_file_path.exists():
                            print(f'original path: {dpath} new_path: {new_file_path}')
                            log.debug(f'original path: {dpath} new_path: {new_file_path}')
                            new_file_path.symlink_to(new_dpath)

