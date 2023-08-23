#!/usr/bin/env python3
from structlog import get_logger
import environs
from pathlib import Path

import common.log_config as log_config
from pub_grouper.pub_grouper import pub_group

log = get_logger()


def main() -> None:
    """Link input paths into the output path."""
    env = environs.Env()
    data_path: Path = env.path('DATA_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    year_index: int = env.int('YEAR_INDEX')
    group_index: int = env.int('GROUP_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    group_metadata_dir: str = env.str('GROUP_METADATA_DIR')
    publoc_key: str = env.str('PUBLOC_KEY')
    link_type: str = env.str('LINK_TYPE')
    
    if link_type == 'SYMLINK':
        symlink=True
    elif link_type == 'COPY':
        symlink=False
    else:
        raise ValueError('LINK_TYPE must be either "SYMLINK" or "COPY"')

    log_config.configure(log_level)

    pub_group(data_path=data_path, out_path=out_path, year_index=year_index, group_index=group_index, 
              data_type_index=data_type_index, group_metadata_dir=group_metadata_dir, publoc_key=publoc_key,
              symlink=symlink)

if __name__ == '__main__':
    main()
