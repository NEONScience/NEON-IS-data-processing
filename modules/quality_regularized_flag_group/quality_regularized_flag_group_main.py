#!/usr/bin/env python3
import environs
from structlog import get_logger

import common.log_config as log_config
from quality_regularized_flag_group.quality_regularized_flag_grouper import QualityRegularizedFlagGrouper

log = get_logger()


def main():
    """Group quality and calibration flags."""
    env = environs.Env()
    regularized_path = env.path('REGULARIZED_PATH')
    quality_path = env.path('QUALITY_PATH')
    out_path = env.path('OUT_PATH')
    log_level = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index = env.int('RELATIVE_PATH_INDEX')
    log_config.configure(log_level)

    log.debug(f'regularized_path: {regularized_path} '
              f'quality_path: {quality_path} '
              f'out_path: {out_path}')

    grouper = QualityRegularizedFlagGrouper(regularized_path=regularized_path,
                                            quality_path=quality_path,
                                            out_path=out_path,
                                            relative_path_index=relative_path_index)
    grouper.group_files()


if __name__ == '__main__':
    main()
