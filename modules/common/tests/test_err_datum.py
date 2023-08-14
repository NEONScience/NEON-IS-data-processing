#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
import inspect
import structlog
import unittest

from common.err_datum import err_datum_path
from common.parse_dir_parts import get_dir_info

log = structlog.get_logger()


class ErrDatumTest(unittest.TestCase):

    def setUp(self):
        self.wrongDirDatm = 'qfs/proc_group/prt/2019/01/01/27134'
        self.DirDatm = 'pfs/proc_group/prt/2019/01/01/27134'
        self.grpDirDatm='pfs/proc_group/2019/01/01/temp-air-single-114/prt/CFGLOC101255'
        self.DirErrBase = 'pfs/proc_group_out/errored_datums/'
        self.DirInInfo = get_dir_info(DirIn = self.DirDatm)
        # extract the repo directory only, self.DirInInfo[3] = 'prt/2019/01/01/27134'
        self.DirErrDir = Path(self.DirErrBase, self.DirInInfo[3])
        self.RmvDatmOut = True
        self.DirOutBase = 'pfs/proc_group_out'

    def test_err_datum(self):
        try:
            x = 4/0
        except:
            # clean up the output directory left from previous testing
            if os.path.exists('pfs'):
                shutil.rmtree('pfs')
            #1. DirDatm does not have 'pfs' no output directory will be created
            err_datum_path(err = 'error testing bad input path #1',DirDatm = self.wrongDirDatm, DirErrBase = self.DirErrBase,
                           RmvDatmOut = self.RmvDatmOut, DirOutBase = self.DirOutBase)
            self.assertFalse(Path(self.DirErrDir).exists())
            #
            #2. DirDatm for Location focus or Sensor focus path
            err_datum_path(err='error testing correct input path #2',DirDatm = self.DirDatm,
                           DirErrBase = self.DirErrBase, RmvDatmOut = True)
            self.assertTrue(Path(self.DirErrDir).exists())
            #3. DirDatm for Group focus path
            # clean up the output directory left from previous testing
            if os.path.exists('pfs'):
                shutil.rmtree('pfs')
            err_datum_path(err='error testing correct input path GROUP focus #3',DirDatm = self.grpDirDatm,
                       DirErrBase = self.DirErrBase,
                       RmvDatmOut = True)
