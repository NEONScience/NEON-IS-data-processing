#!/usr/bin/env python3
""" Level 1 consolidate module
This module consolidates the path structure after data product computations
have been performed to prepare the data for publication.The input path 
structure must be in 'group focus', as follows:
    /YEAR/MONTH/DAY/GROUP_NAME/...
                               GROUP_METADATA/
                               SOURCE_TYPE/
                                  NAMED_LOCATION_ID
                                     DATA_TYPE/
                               DEPENDENT_GROUP/ (optional)
The output structure moves the chosen DATA_TYPE directories to direct children
of the GROUP_NAME directory, retains the GROUP_METADATA directory with group metadata, 
and drops any DEPENDENT_GROUP directories. The output structure is as follows:
    /YEAR/MONTH/DAY/GROUP_NAME/...
                               GROUP_METADATA/
                               DATA_TYPE/
Input parameters are specified in environment variables as follows:
    IN_PATH: The path to the input data, ending at any parent directory up 
        to GROUP_NAME
    OUT_PATH: The base output path that will replace IN_PATH up to 
        RELATIVE_PATH_INDEX
    LOG_LEVEL: The logging level to report at. Options are: 'DEBUG','INFO',
        'WARN','ERROR','FATAL'
    RELATIVE_PATH_INDEX: The starting index of the directory in IN_PATH that 
        will be retained, along with its children (as modified by this code) 
        and place in OUT_PATH.
    GROUP_INDEX: The index of the directory in IN_PATH pertaining to 
        GROUP_NAME in the documentation above.
    GROUP_METADATA_INDEX: The index of the directory in IN_PATH pertaining 
        to group metadata, identifed as GROUP_METADATA in the documentation above.
    GROUP_METADATA_NAMES: The name(s) of the directory pertaining to group 
        metadata, separated by commas. Example ('group','science_review_flags').
    DATA_TYPE_INDEX: The index of the directory in IN_PATH pertaining 
        to DATA_TYPE in the documentation above. 
    DATA_TYPE_NAMES: The name(s) of the director(ies) pertaining to the 
        DATA_TYPEs that should be placed in the output, separated by 
        commas.Example ('stats,quality_metrics,location'). 
""" 
# ---------------------------------------------------------------------------
import environs
import structlog
from pathlib import Path
from collections import namedtuple
import common.log_config as log_config
from level1_consolidate.level1_consolidate_config import Config
from level1_consolidate.level1_consolidate import Level1Consolidate


def main() -> None:
    env = environs.Env()
    in_path: Path = env.path('IN_PATH')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL')
    relative_path_index: int = env.int('RELATIVE_PATH_INDEX')
    group_index: int = env.int('GROUP_INDEX')
    group_metadata_index: int = env.int('GROUP_METADATA_INDEX')
    group_metadata_names: list = env.list('GROUP_METADATA_NAMES')
    data_type_index: int = env.int('DATA_TYPE_INDEX')
    data_type_names: list = env.list('DATA_TYPE_NAMES')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'in_path: {in_path} out_path: {out_path}')
    config = Config(in_path=in_path,
                    out_path=out_path,
                    relative_path_index=relative_path_index,
                    group_index=group_index,
                    group_metadata_index=group_metadata_index,
                    group_metadata_names=group_metadata_names,
                    data_type_index=data_type_index,
                    data_type_names=data_type_names)
    leve1_consolidate = Level1Consolidate(config)
    leve1_consolidate.consolidate_paths()


if __name__ == '__main__':
    main()
