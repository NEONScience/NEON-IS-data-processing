#!/usr/bin/env python3
import re
from pathlib import Path
import structlog


log = structlog.get_logger()


def get_dir_info(DirIn: str):
    """
    Parse input directory into component folders and interpret date embedded in directory structure

    :DirIn:  String. Directory path (often found as an environment variable named as the input repository), structured as
    :follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number (including zero) of
    :parent and child directories of any name, so long as they are not 'pfs' or recognizable as the '/yyyy/mm/dd' structure which
    :indicates the 4-digit year, 2-digit month, and 2-digit day.

    :return A named list:
    :itemize{
    :item dir_in_parts = Character vector of the directory parents and children split into separate character strings, i.e.,
    :('pfs', 'proc_group', 'prt', '2019', '01', '01', '27134')
    :item parent = string, i.e., 'pfs'
    :item repo = Character. The repository name, the child directory of /pfs, i.e., 'proc_group'
    :item idxRepo = Numeric. The index within {dir_in_parts} indicating the position of repo, i.e., 1 in the example.
    :item dirRepo = Character. The directory structure, i.e., prt/2019/01/01/27134
    :item time = POSIXct. The /yyyy/mm/dd date (GMT) embedded within the directory structure (if present). NULL if cannot be interpreted.
    """

    dir_in_info = []
    dir_in_parts = Path(DirIn).parts
    if 'pfs' not in dir_in_parts:
        log.error('pfs directory not found in input path structure. Check input repo.')
    else:
        index_parent_dir = dir_in_parts.index('pfs')
        parent_dir = dir_in_parts[index_parent_dir]
        index_repo = index_parent_dir + 1
        repo = dir_in_parts[index_repo]

        dir_in_length = len(dir_in_parts)
        dir_repo_parts = dir_in_parts[index_repo + 1: dir_in_length]

        # dirRepo = /prt/2019/01/01/27134  string[start:end:step]
        dir_repo = '/'.join(dir_repo_parts)

        if repo is None:
            log.error('Cannot determine repo name. Repository structure must conform to .../pfs/repoName/repoContents.... Check input repo.')

        # Interpret (if possible) the date embedded within the directory structure
        # convert '2019/01/01' to 2019-01-01
        index_time_begin = re.findall(r"[0-9]{4}/[0-9]{2}/[0-9]{2}", DirIn)
        index_time_begin = ''.join(index_time_begin)
        if index_time_begin != -1:
            time =  index_time_begin.replace('/', '-')
        else:
            time = None
    # DirInInfo index starts with 0, which will have parent_dir
        dir_in_info.append(parent_dir)
        dir_in_info.append(index_repo)
        dir_in_info.append(repo)
        dir_in_info.append(dir_repo)
        dir_in_info.append(time)
    # DirInInfo will have the following directories:
    # (parent_dir, repo, index_repo, dirRepo, time)
    return dir_in_info
