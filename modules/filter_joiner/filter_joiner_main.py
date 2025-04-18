#!/usr/bin/env python3
import environs
from pathlib import Path

import common.log_config as log_config

from filter_joiner.joiner import FilterJoiner


def main() -> None:
    env = environs.Env()
    config: str = env.str('CONFIG')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    link_type: str = env.str('LINK_TYPE')
    
    if link_type == 'SYMLINK':
        symlink=True
    elif link_type == 'COPY':
        symlink=False
    else:
        raise ValueError('LINK_TYPE must be either "SYMLINK" or "COPY"')
    
    log_config.configure(log_level)
    filter_joiner = FilterJoiner(config=config, out_path=out_path, relative_path_index=relative_path_index,symlink=symlink)
    filter_joiner.join()


if __name__ == '__main__':
    main()
