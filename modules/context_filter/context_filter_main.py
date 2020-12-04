#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

from common import log_config as log_config
from context_filter.context_filter_config import Config
from context_filter.context_filter import ContextFilter


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    context: str = env.str('CONTEXT')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    trim_index: int = env.int('TRIM_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} out_path: {out_path} context: {context}')
    config = Config(in_path=in_path,
                    out_path=out_path,
                    context=context,
                    trim_index=trim_index,
                    source_id_index=source_id_index,
                    data_type_index=data_type_index)
    context_filter = ContextFilter(config)
    context_filter.filter_files()


if __name__ == '__main__':
    main()
