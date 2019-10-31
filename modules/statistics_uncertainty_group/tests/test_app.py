import os

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from lib import log_config as log_config
import statistics_uncertainty_group.app as app


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')
        self.setUpPyfakefs()
        self.output_path = '/outputs'
        self.statistics_path = \
            '/prt/2018/01/02/CFGLOC112154/stats/prt_CFGLOC112154_2018-01-02_basicStats_001.avro'
        self.uncertainty_path = \
            '/prt/2018/01/02/CFGLOC112154/uncertainty/prt_40202_2018-01-02_uncertainty.json'
        self.uncertainty_fdas_path = \
            '/prt/2018/01/02/CFGLOC112154/uncertainty_fdas/prt_CFGLOC112154_2018-01-02_FDASUncertainty.avro'
        self.fs.create_file(os.path.join('/', 'inputs', self.statistics_path))
        self.fs.create_file(os.path.join('/', 'inputs', self.uncertainty_path))
        self.fs.create_file(os.path.join('/', 'inputs', self.uncertainty_fdas_path))

    def test_group(self):
        app.group(self.statistics_path, self.uncertainty_path, self.uncertainty_fdas_path, self.output_path)
        self.check_output()

    def test_main(self):
        os.environ['STATISTICS_PATH'] = self.statistics_path
        os.environ['UNCERTAINTY_PATH'] = self.uncertainty_path
        os.environ['UNCERTAINTY_FDAS_PATH'] = self.uncertainty_fdas_path
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        self.assertTrue(os.path.lexists(os.path.join(self.output_path, self.statistics_path)))
        self.assertTrue(os.path.lexists(os.path.join(self.output_path, self.uncertainty_path)))
        self.assertTrue(os.path.lexists(os.path.join(self.output_path, self.uncertainty_fdas_path)))


if __name__ == '__main__':
    unittest.main()
