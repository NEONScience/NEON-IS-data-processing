#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config
from path_order.path_order import order_paths


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    indices: list = env.list('PATH_INDICES')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} out_path: {out_path}')
    order_paths(in_path, out_path, indices)


if __name__ == '__main__':
    main()
