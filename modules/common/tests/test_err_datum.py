#!/usr/bin/env python3
import os
import shutil
from pathlib import Path
import inspect
import structlog
import unittest

from common.err_datum import err_datum_path

log = structlog.get_logger()


class ErrDatumTest(unittest.TestCase):

    def setUp(self):
        self.wrongDirDatm = 'qfs/proc_group/prt/2019/01/01/27134'
        self.DirDatm = 'pfs/proc_group/prt/2019/01/01/27134'
        self.grpDirDatm='pfs/proc_group/2019/01/01/temp-air-single-114/prt/CFGLOC101255'
        self.DirErrBase = 'pfs/proc_group_out/errored_datums'
        self.RmvDatmOut = True
        self.DirOutBase = 'pfs/proc_group_out'

    def test_err_datum(self):
        try:
            x = 4/0
        except:
            err_datum_path(err = 'error testing bad input path #1',DirDatm = self.wrongDirDatm, DirErrBase = self.DirErrBase,
                           RmvDatmOut = self.RmvDatmOut, DirOutBase = self.DirOutBase)
            err_datum_path(err='error testing correct input path #2',DirDatm=self.DirDatm,
                           DirErrBase='pfs/out/errored_datums', RmvDatmOut=True)
            err_datum_path(err='error testing correct input path GROUP focus #3',DirDatm = self.grpDirDatm,
                       DirErrBase = self.DirErrBase,
                       RmvDatmOut=True)
