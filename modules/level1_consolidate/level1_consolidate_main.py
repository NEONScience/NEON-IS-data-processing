#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path
from collections import namedtuple
import common.log_config as log_config
from level1_consolidate.level1_consolidate_config import Config
from level1_consolidate.level1_consolidate import Level1Consolidate


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    group_index: int = env.int('GROUP_INDEX')
    group_metadata_index: int = env.int('GROUP_METADATA_INDEX')
    group_metadata_name: str = env.str('GROUP_METADATA_NAME')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    data_type_names: list = env.list('DATA_TYPE_NAMES')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} out_path: {out_path}')
    config = Config(in_path=in_path,
                    out_path=out_path,
                    relative_path_index=relative_path_index,
                    group_index=group_index,
                    group_metadata_index=group_metadata_index,
                    group_metadata_name=group_metadata_name,
                    data_type_index=data_type_index,
                    data_type_names=data_type_names)
    leve1_consolidate = Level1Consolidate(config)
    leve1_consolidate.consolidate_paths()


if __name__ == '__main__':
    main()
