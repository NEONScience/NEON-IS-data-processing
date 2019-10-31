import os

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from lib import log_config as log_config
import quality_metrics_statistics_group.app as app


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')
        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'inputs')
        self.output_path = os.path.join('/', 'outputs')
        self.metadata_path = os.path.join('/', 'prt', '2018', '01', '02', 'CFGLOC112154')

        self.quality_path = os.path.join(self.metadata_path, 'stats')
        self.statistics_path = os.path.join(self.metadata_path, 'quality_metrics')

        self.statistics_path_1 = os.path.join(self.statistics_path,
                                              'prt_CFGLOC112154_2018-01-02_basicStats_001.avro')
        self.statistics_path_2 = os.path.join(self.statistics_path,
                                              'prt_CFGLOC112154_2018-01-02_basicStats_030.avro')
        self.quality_path_1 = os.path.join(self.quality_path,
                                           'prt_CFGLOC112154_2018-01-02_qualityMetrics_001.avro')
        self.quality_path_2 = os.path.join(self.quality_path,
                                           'prt_CFGLOC112154_2018-01-02_qualityMetrics_030.avro')
        self.fs.create_file(os.path.join(self.input_path, self.statistics_path_1))
        self.fs.create_file(os.path.join(self.input_path, self.statistics_path_2))
        self.fs.create_file(os.path.join(self.input_path, self.quality_path_1))
        self.fs.create_file(os.path.join(self.input_path, self.quality_path_2))

    def test_group(self):
        app.group(self.statistics_path, self.quality_path, self.output_path)
        self.check_output()

    def test_main(self):
        os.environ['STATISTICS_PATH'] = self.statistics_path
        os.environ['QUALITY_PATH'] = self.quality_path
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        self.assertTrue(os.path.lexists(os.path.join(self.output_path, self.statistics_path_1)))
        self.assertTrue(os.path.lexists(os.path.join(self.output_path, self.statistics_path_2)))
        self.assertTrue(os.path.lexists(os.path.join(self.output_path, self.quality_path_1)))
        self.assertTrue(os.path.lexists(os.path.join(self.output_path, self.quality_path_2)))


if __name__ == '__main__':
    unittest.main()
