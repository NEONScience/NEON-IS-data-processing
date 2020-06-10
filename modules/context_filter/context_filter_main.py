#!/usr/bin/env python3
import environs
import structlog

from common import log_config as log_config
from context_filter.context_filter import ContextFilter


def main():
    env = environs.Env()
    in_path = env.path('IN_PATH')
    out_path = env.path('OUT_PATH')
    context = env.str('CONTEXT')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    source_type_index = env.int('SOURCE_TYPE_INDEX')
    year_index = env.int('YEAR_INDEX')
    month_index = env.int('MONTH_INDEX')
    day_index = env.int('DAY_INDEX')
    source_id_index = env.int('SOURCE_ID_INDEX')
    data_type_index = env.int('DATA_TYPE_INDEX')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} out_path: {out_path} context: {context}')

    context_filter = ContextFilter(input_path=in_path,
                                   output_path=out_path,
                                   context=context,
                                   source_type_index=source_type_index,
                                   year_index=year_index,
                                   month_index=month_index,
                                   day_index=day_index,
                                   source_id_index=source_id_index,
                                   data_type_index=data_type_index)
    context_filter.filter()


if __name__ == '__main__':
    main()
