#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

from common import log_config as log_config
from context_filter.data_file_path import DataFilePath
from context_filter.context_filter import ContextFilter


def main():
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    context: str = env.str('CONTEXT')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index: int = env.int('SOURCE_TYPE_INDEX')
    year_index: int = env.int('YEAR_INDEX')
    month_index: int = env.int('MONTH_INDEX')
    day_index: int = env.int('DAY_INDEX')
    source_id_index: int = env.int('SOURCE_ID_INDEX')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} out_path: {out_path} context: {context}')

    data_file_path = DataFilePath(source_type_index=source_type_index,
                                  year_index=year_index,
                                  month_index=month_index,
                                  day_index=day_index,
                                  source_id_index=source_id_index,
                                  data_type_index=data_type_index)
    context_filter = ContextFilter(input_path=in_path,
                                   output_path=out_path,
                                   context=context,
                                   data_file_path=data_file_path)
    context_filter.filter()


if __name__ == '__main__':
    main()
