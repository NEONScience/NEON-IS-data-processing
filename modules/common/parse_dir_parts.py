#!/usr/bin/env python3
import re
from pathlib import Path
import structlog

log = structlog.get_logger()

def get_dir_info(DirIn: Path):

    """
    Parse Pachyderm directory into component folders and interpret date embedded in directory structure

    :DirIn:  String. Directory path (often found as an environment variable named as the input repository), structured as
    :follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number (including zero) of
    :parent and child directories of any name, so long as they are not 'pfs' or recognizable as the '/yyyy/mm/dd' structure which
    :indicates the 4-digit year, 2-digit month, and 2-digit day.

    :return A named list:
    :itemize{
    :item DirIn_parts = Character vector of the directory parents and children split into separate character strings, i.e.,
    :('pfs', 'proc_group', 'prt', '2019', '01', '01', '27134')
    :item parent = string, i.e., 'pfs'
    :item repo = Character. The repository name, the child directory of /pfs, i.e., 'proc_group'
    :item idxRepo = Numeric. The index within {DirIn_parts} indicating the position of repo, i.e., 1 in the example.
    :item dirRepo = Character. The directory structure, i.e., prt/2019/01/01/27134
    :item time = POSIXct. The /yyyy/mm/dd date (GMT) embedded within the directory structure (if present). NULL if cannot be interpreted.
    """

    DirInInfo =[]
    DirIn_parts = Path(DirIn).parts
    if 'pfs' not in DirIn_parts:
        log.error('\t\tpfs directory not found in input path structure. Check input repo.')
    else:
        IdxParentDir = DirIn_parts.index("pfs")
        parent_dir = DirIn_parts[IdxParentDir]
        IdxRepo = IdxParentDir + 1
        repo = DirIn_parts[IdxRepo]

        DirIn_len = len(DirIn_parts)
        DirRepo_parts = DirIn_parts[IdxRepo + 1: DirIn_len]

    # dirRepo = /prt/2019/01/01/27134  string[start:end:step]
        DirRepo = '/'.join(DirRepo_parts)
        DirInInfo.append(parent_dir)
        DirInInfo.append(IdxRepo)
        DirInInfo.append(repo)
        DirInInfo.append(DirRepo)

        if repo == None:
            log.error('Cannot determine repo name. Repository structure must conform to .../pfs/repoName/repoContents.... Check input repo.')

    # Interpret (if possible) the date embedded within the directory structure
    # convert '2019/01/01' to 2019-01-01

        idxTimeBgn = re.findall(r"[0-9]{4}/[0-9]{2}/[0-9]{2}", DirIn)
        idxTimeBgn = ''.join(idxTimeBgn)
        if idxTimeBgn != -1:
            time =  idxTimeBgn.replace('/', '-')
        else:
            time = None
        DirInInfo.append(time)
    # # DirInInfo will have the following directories
    # (parent_dir, repo, IdxRepo, dirRepo, time)
    return DirInInfo
