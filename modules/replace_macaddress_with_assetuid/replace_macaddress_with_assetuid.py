#!usr/bin/env python3
import os
import logging
from pathlib import Path
from contextlib import closing
from structlog import get_logger
import os

log = get_logger()


def load_assetuid(data_path: Path, map_path: Path, out_path: Path, source_type:str) -> None:

    for dpath in Path("/pfs/DATA_PATH/").rglob('*'):
        if Path(dpath).is_file():
            mac_address = Path(dpath).stem.split("_")[1]
            new_dpath = os.path.normpath(dpath)
            print(f'Mac_Address name is: {mac_address}')
            path_list = new_dpath.split(os.sep)
            source_type = source_type.split("_")[0]
            IdxSourceType = path_list.index(source_type)
            for mpath in Path(map_path).rglob('*'):
                if Path(mpath).is_file():
                    if (Path(mpath).stem.split("_")[2] == mac_address):
                        with open(Path(mpath), 'r') as f:
                            asset_uid = f.read().split()[0]
                            print(f'Asset_UID name is: {asset_uid}')
                        f.close()
                        extended_path = os.sep.join(path_list[IdxSourceType:]).replace(str(mac_address), str(asset_uid))
                        print(f'Extended Path is: {extended_path}')
                        new_file_path = Path(os.path.join(Path(out_path), extended_path))
                        Path(new_file_path).parent.mkdir(parents=True, exist_ok=True)
                        if not new_file_path.exists():
                            print(f'original path: {dpath} new_path: {new_file_path}')
                            log.debug(f'original path: {dpath} new_path: {new_file_path}')
                            new_file_path.symlink_to(new_dpath)

