#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import qaqc_regularized_flag_group.app as app


class QaqcRegularizedFlagGroupTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

        self.in_path = os.path.join('/', 'inputs')
        self.out_path = os.path.join('/', 'outputs')
        self.regularized_path = os.path.join(self.in_path, 'regularized')
        self.quality_path = os.path.join(self.in_path, 'quality')

        #  regularized file
        self.fs.create_file(os.path.join(self.regularized_path, 'prt', '2018', '01', '01',
                                         'CFGLOC112154', 'flags', 'prt_CFGLOC112154_2018-01-01_flagsCal.ext'))
        #  quality file
        self.fs.create_file(os.path.join(self.quality_path, 'prt', '2018', '01', '01',
                                         'CFGLOC112154', 'flags', 'prt_CFGLOC112154_2018-01-01_plausibility.ext'))
        # quality file 2
        self.fs.create_file(os.path.join(self.quality_path, 'prt', '2018', '01', '02',
                                         'CFGLOC112154', 'flags', 'prt_CFGLOC112154_2018-01-01_plausibility.ext'))

        self.relative_path_index = 3

    def test_main(self):
        os.environ['REGULARIZED_PATH'] = self.regularized_path
        os.environ['QUALITY_PATH'] = self.quality_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        app.main()
        self.check_output()

    def check_output(self):
        regularized_path = os.path.join(self.out_path, 'prt', '2018', '01', '01',
                                        'CFGLOC112154', 'flags', 'prt_CFGLOC112154_2018-01-01_flagsCal.ext')
        quality_path = os.path.join(self.out_path, 'prt', '2018', '01', '01',
                                    'CFGLOC112154', 'flags', 'prt_CFGLOC112154_2018-01-01_plausibility.ext')

        quality_path_2 = os.path.join(self.out_path, 'prt', '2018', '01', '02',
                                      'CFGLOC112154', 'flags', 'prt_CFGLOC112154_2018-01-01_plausibility.ext')

        self.assertTrue(os.path.lexists(regularized_path))
        self.assertTrue(os.path.lexists(quality_path))
        # file on different day should be excluded from output
        self.assertFalse(os.path.lexists(quality_path_2))
