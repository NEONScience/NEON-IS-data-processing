#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
import inspect
import structlog

log = structlog.get_logger()


def err_datum_path(err: str,DirDatm: Path,DirErrBase: Path,RmvDatmOut: bool,DirOutBase=None) -> None:
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
    log.info(f'Error resulted from call: {caller}')

    # Inform the user of the error routing
    log.info(f'Re-routing failed datum path {DirDatm} to {DirErrBase}')
    DirDatm_parts = Path(DirDatm).parts
    DirDatm_len = len(DirDatm_parts)
    IdxRepo = DirDatm_parts.index("pfs") + 1
    DirRepo = DirDatm_parts[IdxRepo + 1: DirDatm_len]

    # dirRepo_path = /prt/2019/01/01/27134  string[start:end:step]
    DirRepo_path = '/'.join(DirRepo)
    if DirOutBase == None:
        DirOutBase = Path(DirErrBase).parents[0]

    # Write an empty file
    DirErr_path = Path(DirErrBase,DirRepo_path)
    DirOut_path = Path(DirOutBase,DirRepo_path)
    os.makedirs(DirErr_path,exist_ok=True)
    Err_file = os.path.join(DirErr_path,os.path.basename(DirErr_path).split('/')[-1])
    file1 = open(Err_file,"w")
    file1.close()

    # Remove any partial output for the datum
    if (RmvDatmOut == True):
        if os.path.exists(DirOut_path):
            removed = shutil.rmtree(DirOut_path)
            log.info(f'Removed partial output for errored datum:  {DirOut_path}')
            print('\n\t ====== removed: ',removed)
