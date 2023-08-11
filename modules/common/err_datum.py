#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
import inspect
import structlog

log = structlog.get_logger()
from common.parse_dir_parts import get_dir_info


def err_datum_path(err: str,DirDatm: Path,DirErrBase: Path,RmvDatmOut: bool,DirOutBase=None) -> None:
    """
    Route datum errors to a specified location.

    :param err: The error condition returned from a function call.
    :param DirDatm: The file path, e.g., 'pfs/proc_group/prt/2019/01/01/27134'.
    :param DirErrBase: The erred file path, e.g., 'pfs/proc_group_output/errored_datums'.
    :param DirOutBase: The output file path, e.g., 'pfs/proc_group_output'.
    :return: The action of creating the path structure to the datum within DirErrBase, having replaced
    :        the #/pfs/BASE_REPO portion of DirDatm with DirErrBase.
    """

    caller = inspect.stack()[1].function
    log.info(f'The error, "{err}", resulted from call: {caller}. ')
    log.info(f'Rerouting starts..... ')

    # call to parse input directory
    DirInInfo = get_dir_info(DirIn = DirDatm)
    # DirInInfo will have the following directories, DirInInfo[0] = parent_dir,
    # DirInInfo[1] = repo, DirInInfo[2] = IdxRepo, DirInInfo[3] = DirRepo, DirInInfo[4] = time.

    if DirOutBase == None:
        DirOutBase = Path(DirErrBase).parents[0]
    #
    if DirInInfo == []:
        log.info(f'Re-routing stopped due to the input path structure not compliant.........\n')

    else:
        # Inform the user of the error routing
        log.info(f'Re-routing failed datum .........')
        log.info(f'      from: {DirDatm}')
        DirRepo = DirInInfo[3]
        DirErr_path = Path(DirErrBase,DirRepo)
        log.info(f'      to: {DirErr_path}')
        DirOut_path = Path(DirOutBase,DirRepo)
        os.makedirs(DirErr_path,exist_ok=True)
        Err_file = os.path.join(DirErr_path,os.path.basename(DirErr_path).split('/')[-1])
    # Write an empty file
        file1 = open(Err_file,"w")
        file1.close()
        log.info(f'An empty file is written in:  {DirErr_path}')
        log.info(f'Rerouting completed successfully..... \n')
    #
    # Remove any partial output for the datum
        if (RmvDatmOut == True):
            if os.path.exists(DirOut_path):
                removed = shutil.rmtree(DirOut_path)
                log.info(f'Removed partial output for errored datum:  {DirOut_path}')
