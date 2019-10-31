import os
import pathlib

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from lib import log_config as log_config
import threshold_regularized_group.app as app


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')
        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'inputs')
        self.output_path = os.path.join('/', 'outputs')

        self.metadata_path = pathlib.Path('prt/2019/05/24/0001')

        self.data_dir = 'data'
        self.location_dir = 'locations'
        self.threshold_dir = 'thresholds'

        self.data_file = 'data.avro'
        self.location_file = 'locations.json'
        self.threshold_file = 'threshold.json'

        self.regularized_path = os.path.join(self.input_path, 'regularized', self.metadata_path)
        self.threshold_path = os.path.join(self.input_path, self.threshold_dir, self.metadata_path)

        #  regularized data file
        self.fs.create_file(os.path.join(self.regularized_path, self.data_dir, self.data_file))
        #  location file
        self.fs.create_file(os.path.join(self.threshold_path, self.location_dir, self.location_file))
        #  threshold file
        self.fs.create_file(os.path.join(self.threshold_path, self.threshold_dir, self.threshold_file))

    def test_group(self):
        app.group(self.regularized_path, self.threshold_path, self.output_path)
        self.check_output()

    def test_main(self):
        os.environ['REGULARIZED_PATH'] = self.regularized_path
        os.environ['THRESHOLD_PATH'] = self.threshold_path
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        root = os.path.join(self.output_path, self.metadata_path)
        data_path = os.path.join(root, self.data_dir, self.data_file)
        locations_path = os.path.join(root, self.location_dir, self.location_file)
        threshold_path = os.path.join(root, self.threshold_dir, self.threshold_file)
        self.assertTrue(os.path.lexists(data_path))
        self.assertTrue(os.path.lexists(locations_path))
        self.assertTrue(os.path.lexists(threshold_path))


if __name__ == '__main__':
    unittest.main()
