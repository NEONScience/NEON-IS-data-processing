#!usr/bin/env python3
from pathlib import Path
from structlog import get_logger
import common.log_config as log_config
import os
from replace_macaddress_with_assetuid import load_assetuid

log = get_logger()


def main() -> None:
    data_path: Path = os.environ['DATA_PATH']
    map_path: Path = os.environ['MAP_PATH']
    out_path: Path = os.environ['OUT_PATH']
    source_type: str = os.environ['SOURCE_TYPE']
    relative_path_index: int = int(os.environ['RELATIVE_PATH_INDEX'])
    log_level: str = os.environ['LOG_LEVEL']
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')
    load_assetuid(data_path = data_path, map_path = map_path, out_path = out_path, source_type = source_type, relative_path_index = relative_path_index)


if __name__ == '__main__':
    main()
