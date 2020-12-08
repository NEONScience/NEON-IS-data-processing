#!/usr/bin/env python3
import environs
from structlog import get_logger
from pathlib import Path

import common.log_config as log_config
from quality_regularized_flag_group.quality_regularized_flag_grouper import QualityRegularizedFlagGrouper

log = get_logger()


def main() -> None:
    """Group quality and regularized data flags."""
    env = environs.Env()
    regularized_path: Path = env.path('REGULARIZED_PATH')
    quality_path: Path = env.path('QUALITY_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
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
