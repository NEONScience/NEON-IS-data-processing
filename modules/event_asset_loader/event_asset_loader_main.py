#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from event_asset_loader.event_asset_loader import EventAssetLoader

log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    source_path: Path = env.path('SOURCE_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    log_config.configure(log_level)
    log.debug(f'source_path: {source_path} out_path: {out_path}')
    event_asset_loader = EventAssetLoader(source_path=source_path,
                                          out_path=out_path,
                                          source_type_index=source_type_index,
                                          source_id_index=source_id_index)
    event_asset_loader.link_event_files()


if __name__ == '__main__':
    main()
