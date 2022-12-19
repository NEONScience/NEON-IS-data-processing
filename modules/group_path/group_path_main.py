#!/usr/bin/env python3
""" Group path module
The group path module brings together the members of a (data product) group
and inserts the group name into the path structure so that follows the 
convention:
    /YEAR/MONTH/DAY/GROUP_NAME/...

The baseline input is the path to the group assignment repository, which 
in the format:
    /GROUP_PREFIX/YEAR/MONTH/DAY/GROUP_MEMBER/group/group_member_file.json
The group assignment repository includes information for each member of a 
group that matches the group prefix. Any days that a group is active are 
populated with a file for each member of the group. The group_member_file.json
will be named for the group member and includes all the groups that the member
is included in for that day (limited to those groups that match the 
GROUP_PREFIX). 

The path to the group assignment repository is required. The
group file will be read in order to learn the GROUP_NAMEs that the 
GROUP_MEMBER is in for the day. The group_member_file.json will be placed in 
the output repository in the following location:
    /YEAR/MONTH/DAY/GROUP_NAME/group/group_member_file.json
Thus, if there are multiple members of the GROUP_NAME, there will be a file for 
each member.

The paths of two additional types of repository structures are optional inputs
to this module and serve the purpose of joining the data from each group member 
into the same group directory in the output.

Location focus path: A repository that is organized like:
    /SOURCE_TYPE/YEAR/MONTH/DAY/NAMED_LOCATION/...
Any repository contents for a NAMED_LOCATION that is a member of an active
GROUP_NAME matching the GROUP_PREFIX as found in the group assignment path will 
be placed in the output for the group at the following location:
     /YEAR/MONTH/DAY/GROUP_NAME/NAMED_LOCATION/...

Group focus path: A repository that is organized like:
    /YEAR/MONTH/DAY/DEPENDENT_GROUP_NAME/...
This repository is already organized in a group structure, but including groups 
that are potential members of the groups with GROUP_PREFIX. Any repository 
contents for the DEPENDENT_GROUP_NAME that is a member of an active
group matching the GROUP_PREFIX as found in the group assignment path will be 
placed in the output for the group at the following location:
     /YEAR/MONTH/DAY/GROUP_NAME/DEPENDENT_GROUP_NAME/...

""" 
# ---------------------------------------------------------------------------
from structlog import get_logger
import environs
import os
from pathlib import Path

import common.log_config as log_config

from group_path.group_path_config import Config
from group_path.group_path import GroupPath

log = get_logger()


def main() -> None:
    """Add the location group name from the location file into the path."""
    env = environs.Env()
    group_assignment_path: Path = env.path('GROUP_ASSIGNMENT_PATH')
    if 'LOCATION_FOCUS_PATH' in os.environ:
        location_focus_path: Path = env.path('LOCATION_FOCUS_PATH')
    else:
        location_focus_path: Path = None
        
    if 'GROUP_FOCUS_PATH' in os.environ:
        group_focus_path: Path = env.path('GROUP_FOCUS_PATH')
    else: 
        group_focus_path: Path = None
        
    group: str = env.str('GROUP')
    out_path: Path = env.path('OUT_PATH')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    group_assignment_year_index: int = env.int('GROUP_ASSIGNMENT_YEAR_INDEX')
    group_assignment_month_index: int = env.int('GROUP_ASSIGNMENT_MONTH_INDEX')
    group_assignment_day_index: int = env.int('GROUP_ASSIGNMENT_DAY_INDEX')
    group_assignment_member_index: int = env.int('GROUP_ASSIGNMENT_MEMBER_INDEX')
    group_assignment_data_type_index: int = env.int('GROUP_ASSIGNMENT_DATA_TYPE_INDEX')
    if location_focus_path is not None:
        location_focus_source_type_index: int = env.int('LOCATION_FOCUS_SOURCE_TYPE_INDEX')
        location_focus_year_index: int = env.int('LOCATION_FOCUS_YEAR_INDEX')
        location_focus_month_index: int = env.int('LOCATION_FOCUS_MONTH_INDEX')
        location_focus_day_index: int = env.int('LOCATION_FOCUS_DAY_INDEX')
        location_focus_location_index: int = env.int('LOCATION_FOCUS_LOCATION_INDEX')
    else:
        location_focus_source_type_index: int = None
        location_focus_year_index: int = None
        location_focus_month_index: int = None
        location_focus_day_index: int = None
        location_focus_location_index: int = None

    if group_focus_path is not None:
        group_focus_year_index: int = env.int('GROUP_FOCUS_YEAR_INDEX')
        group_focus_month_index: int = env.int('GROUP_FOCUS_MONTH_INDEX')
        group_focus_day_index: int = env.int('GROUP_FOCUS_DAY_INDEX')
        group_focus_group_index: int = env.int('GROUP_FOCUS_GROUP_INDEX')
    else:
        group_focus_year_index: int = None
        group_focus_month_index: int = None
        group_focus_day_index: int = None
        group_focus_group_index: int = None

    log_config.configure(log_level)
    log.debug(f'location_focus_path: {location_focus_path} group_assignment_path: {group_assignment_path} group: {group} out_path: {out_path}')
    config = Config(group_assignment_path=group_assignment_path,
                    location_focus_path=location_focus_path,
                    group_focus_path=group_focus_path,
                    out_path=out_path,
                    group=group,
                    group_assignment_year_index=group_assignment_year_index,
                    group_assignment_month_index=group_assignment_month_index,
                    group_assignment_day_index=group_assignment_day_index,
                    group_assignment_member_index=group_assignment_member_index,
                    group_assignment_data_type_index=group_assignment_data_type_index,
                    location_focus_source_type_index=location_focus_source_type_index,
                    location_focus_year_index=location_focus_year_index,
                    location_focus_month_index=location_focus_month_index,
                    location_focus_day_index=location_focus_day_index,
                    location_focus_location_index=location_focus_location_index,
                    group_focus_year_index=group_focus_year_index,
                    group_focus_month_index=group_focus_month_index,
                    group_focus_day_index=group_focus_day_index,
                    group_focus_group_index=group_focus_group_index)
    group_path = GroupPath(config)
    group_path.add_groups_to_paths()


if __name__ == '__main__':
    main()
