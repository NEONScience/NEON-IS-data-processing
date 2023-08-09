#!/usr/bin/env python3
import os
import string
from pathlib import Path
from typing import List, Tuple
from datetime import datetime
import inspect
from structlog import get_logger
import structlog

log = structlog.get_logger()


def err_datum_path(err: str, DirDatm: Path, DirErrBase: Path, DirOutBase = None) -> None:
    """
    Parse a datum path.

    :param err: The error condition returned from a function call.
    :param DirDatm: The file path, e.g., 'pfs/proc_group/prt/2019/01/01/27134'.
    :param DirErrBase: The erred file path, e.g., 'pfs/proc_group_output/errored_datums'.
    :param DirOutBase: The output file path, e.g., 'pfs/proc_group_output'.
    :return: The action of creating the path structure to the datum within DirErrBase, having replaced
    :        the #/pfs/BASE_REPO portion of DirDatm with DirErrBase.
    """

    log.debug(err)
    caller = inspect.stack()[1].function
    log.debug(f'Error resulted from call: {caller}')
    DirDatm_parts = Path(DirDatm).parts
    DirDatm_len = len(DirDatm_parts)
    idxRepo = DirDatm_parts.index("pfs") + 1
    dirRepo = DirDatm_parts[idxRepo+1 : DirDatm_len]
    # dirRepo_path = /prt/2019/01/01/27134  string[start:end:step]
    dirRepo_path = '/'.join(dirRepo)
    if DirOutBase == '':
        DirOutBase = Path(DirErrBase).parents[0]

    # Write an empty file
    DirErr_path = Path(DirErrBase, dirRepo_path)
    os.makedirs(DirErr_path, exist_ok=True)
    Err_file = os.path.join(DirErr_path, os.path.basename(DirErr_path).split('/')[-1])
    file1 = open(Err_file, "w")
    file1.close()
