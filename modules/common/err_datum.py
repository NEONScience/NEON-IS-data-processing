import os
import string
from pathlib import Path
from typing import List, Tuple
from datetime import datetime
import inspect


def err_datum_path(err: str, DirDatm: Path, DirErrBase: Path, DirOutBase: Path) -> None:
    """
    Parse a datum path.

    :param err: The error condition returned from a function call.
    :param DirDatm: The file path, e.g., 'pfs/proc_group/prt/2019/01/01/27134'.
    :param DirErrBase: The erred file path, e.g., 'pfs/proc_group_output/errored_datums'.
    :param DirOutBase: The output file path, e.g., 'pfs/proc_group_output'.
    :return: The action of creating the path structure to the datum within DirErrBase, having replaced
    :        the #/pfs/BASE_REPO portion of DirDatm with DirErrBase.
    """
    DirDatm_parts = Path(DirDatm).parts
    print('\n\t ====== DirDatm_parts: ', DirDatm_parts)
    DirDatm_len = len(DirDatm_parts)
    idxRepo = DirDatm_parts.index("pfs") + 1
    dirRepo = DirDatm_parts[idxRepo+1 : DirDatm_len]
    # dirRepo_path = /prt/2019/01/01/27134  string[start:end:step]
    dirRepo_path = '/'.join(dirRepo)
    print('\n\t ====== dirRepo_path: ',dirRepo_path)
    if DirOutBase == '':
        DirOutBase = Path(DirErrBase).parents[0]
    print('\n\t ====== DirOutBase: ', DirOutBase)
    print(inspect.stack()[0].function)
    print(inspect.stack()[1].function)
    print( inspect.stack()[1][3])
    print(os.path.basename(__file__))

    # Write an empty file
    DirErr_path = Path(DirErrBase, dirRepo_path)
    os.makedirs(DirErr_path)
    print('\n\t DirErr_path:',DirErr_path)
    Err_file = os.path.join(DirErr_path, os.path.basename(DirErr_path).split('/')[-1])
    print('\n\t Err_file:', Err_file)
    file1 = open(Err_file, "w")
    file1.close()


err_datum_path(err = 'error', DirDatm = 'pfs/proc_group/prt/2019/01/01/27134',
                                   DirErrBase = 'pfs/proc_group_out/erred_datums',
                                   DirOutBase = 'pfs/proc_group_out')
