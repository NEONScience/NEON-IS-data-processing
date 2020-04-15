#!/usr/bin/env python3
import environs
import structlog

from lib import log_config as log_config
from context_filter.filter import ContextFilter


def main():
    env = environs.Env()
    in_path = env('IN_PATH')
    out_path = env('OUT_PATH')
    context = env('CONTEXT')
    log_level = env('LOG_LEVEL')
    source_type_index = env('SOURCE_TYPE_INDEX')
    year_index = env('YEAR_INDEX')
    month_index = env('MONTH_INDEX')
    day_index = env('DAY_INDEX')
    source_id_index = env('SOURCE_ID_INDEX')
    data_type_index = env('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path}')
    log.debug(f'out_path: {out_path}')
    log.debug(f'context: {context}')
    context_filter = ContextFilter(int(source_type_index), int(year_index), int(month_index),
                                   int(day_index), int(source_id_index), int(data_type_index))
    context_filter.filter(in_path, out_path, context)


if __name__ == '__main__':
    main()
