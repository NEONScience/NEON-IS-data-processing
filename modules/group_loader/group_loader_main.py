#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path
from contextlib import closing
from functools import partial

import common.log_config as log_config
from data_access.db_connector import connect
from data_access.get_groups import get_groups
from group_loader.group_loader import load_groups

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    group_prefix: str = env.str('GROUP_PREFIX')
    db_url: str = env.str('DATABASE_URL')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)

    with closing(connect(db_url)) as connection:
        get_member_groups_partial = partial(get_member_groups, connection=connection)
        load_groups(out_path=out_path, get_groups=get_member_groups_partial, group_prefix=group_prefix)

if __name__ == "__main__":
    main()
