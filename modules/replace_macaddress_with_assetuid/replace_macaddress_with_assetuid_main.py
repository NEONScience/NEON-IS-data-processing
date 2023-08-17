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
    log_level: str = os.environ['LOG_LEVEL']
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')
    print(f'final output path is: {Path(out_path)}')
    load_assetuid(data_path = data_path, map_path = map_path, out_path = out_path)


if __name__ == '__main__':
    main()
