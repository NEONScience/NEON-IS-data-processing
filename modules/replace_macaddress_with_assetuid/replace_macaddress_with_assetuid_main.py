#!usr/bin/env python3
from pathlib import Path
from structlog import get_logger

import common.log_config as log_config
import environs

from replace_macaddress_with_assetuid.replace_macaddress_with_assetuid import load_assetuid

log = get_logger()


def main() -> None:
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    map_path: Path = env.path('MAP_PATH')
    out_path: Path = env.path('OUT_PATH')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    load_assetuid(data_path=data_path, map_path=map_path, out_path=out_path, relative_path_index=relative_path_index)


if __name__ == '__main__':
    main()
