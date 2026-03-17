#!/usr/bin/env python3
import shutil
from pathlib import Path
import structlog
import unittest

from common.err_datum import err_datum_path
from common.parse_dir_parts import get_dir_info


log = structlog.get_logger()


class ErrDatumTest(unittest.TestCase):

    def setUp(self):
        self.out_dir = Path('pfs')
        self.wrong_dir_datum = Path('qfs/proc_group/prt/2019/01/01/27134')
        self.dir_datum = 'pfs/proc_group/prt/2019/01/01/27134'
        self.group_dir_datum= 'pfs/proc_group/2019/01/01/temp-air-single-114/prt/CFGLOC101255'
        self.dir_err_base = Path('pfs/proc_group_out/errored_datums')
        self.dir_in_info = get_dir_info(DirIn = self.dir_datum)
        # Extract the repo directory only, self.DirInInfo[3] = 'prt/2019/01/01/27134'
        self.dir_err_dir = Path(self.dir_err_base, self.dir_in_info[3])
        self.remove_datum_out = True
        self.dir_out_base = Path('pfs/proc_group_out')

    def test_err_datum(self):
        # Clean up the output directory left from previous testing.
        clean(self.out_dir)

        # 1. DirDatm does not have 'pfs' so no output directory will be created.
        err_datum_path(
            err = 'error testing bad input path #1',
            DirDatm = self.wrong_dir_datum,
            DirErrBase = self.dir_err_base,
            RmvDatmOut = self.remove_datum_out,
            DirOutBase = self.dir_out_base
        )
        self.assertFalse(Path(self.dir_err_dir).exists())

        # 2. DirDatm for Location focus or Sensor focus path.
        err_datum_path(
            err='error testing correct input path #2',
            DirDatm = self.dir_datum,
            DirErrBase = self.dir_err_base,
            RmvDatmOut = True
        )
        self.assertTrue(Path(self.dir_err_dir).exists())

        # 3. DirDatm for Group focus path.
        # Clean up the output directory left from previous testing.
        clean(self.out_dir)
        err_datum_path(
            err='error testing correct input path GROUP focus #3',
            DirDatm = self.group_dir_datum,
            DirErrBase = self.dir_err_base,
            RmvDatmOut = True
        )
        # Final clean up.
        clean(self.out_dir)


def clean(path: Path) -> None:
    if Path.exists(path):
        shutil.rmtree(path)
