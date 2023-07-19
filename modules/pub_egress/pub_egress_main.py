#!/usr/bin/env python3
import environs
from structlog import get_logger
from pathlib import Path

from common import log_config as log_config
from pub_egress.pub_egress import Pub_egress


def main() -> None:
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    starting_path_index: int = env.int('STARTING_PATH_INDEX')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    egress_url: str = env.str('EGRESS_URL')
    log_config.configure(log_level)
    log = get_logger()
    log.debug(f'data_dir: {data_path}')
    log.debug(f'out_dir: {out_path}')

    egress = Pub_egress(data_path, starting_path_index, out_path, egress_url)
    egress.upload()


if __name__ == '__main__':
    main()
