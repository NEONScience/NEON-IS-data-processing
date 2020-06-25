#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import quality_regularized_flag_group.quality_regularized_flag_group_main as quality_regularized_flag_group_main


class QaqcRegularizedFlagGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.in_path = Path('/inputs')
        self.out_path = Path('/outputs')
        self.regularized_path = Path(self.in_path, 'regularized')
        self.quality_path = Path(self.in_path, 'quality')
        #  regularized file
        self.fs.create_file(Path(self.regularized_path, 'prt/2018/01/01/CFGLOC112154/flags/',
                                 'prt_CFGLOC112154_2018-01-01_flagsCal.ext'))
        #  quality file
        self.fs.create_file(Path(self.quality_path, 'prt/2018/01/01/CFGLOC112154/flags',
                                 'prt_CFGLOC112154_2018-01-01_plausibility.ext'))
        # quality file 2
        self.fs.create_file(Path(self.quality_path, 'prt/2018/01/02/CFGLOC112154/flags',
                                 'prt_CFGLOC112154_2018-01-01_plausibility.ext'))
        self.relative_path_index = 3

    def test_main(self):
        os.environ['REGULARIZED_PATH'] = str(self.regularized_path)
        os.environ['QUALITY_PATH'] = str(self.quality_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        quality_regularized_flag_group_main.main()
        self.check_output()

    def check_output(self):
        regularized_path = Path(self.out_path, 'prt/2018/01/01/CFGLOC112154/flags',
                                'prt_CFGLOC112154_2018-01-01_flagsCal.ext')
        quality_path = Path(self.out_path, 'prt/2018/01/01/CFGLOC112154/flags',
                            'prt_CFGLOC112154_2018-01-01_plausibility.ext')
        quality_path_2 = Path(self.out_path, 'prt/2018/01/02/CFGLOC112154/flags',
                              'prt_CFGLOC112154_2018-01-01_plausibility.ext')
        self.assertTrue(regularized_path.exists())
        self.assertTrue(quality_path.exists())
        # file on different day should be excluded from output
        self.assertFalse(quality_path_2.exists())
