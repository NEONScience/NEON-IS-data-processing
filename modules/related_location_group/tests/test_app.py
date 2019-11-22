import os

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

from lib import log_config as log_config
import related_location_group.app as app


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')
        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'repo', 'inputs')
        self.output_path = os.path.join('/', 'outputs')

        self.metadata_path = os.path.join('2019', '05', '24', '0001', 'CFGLOC123')

        self.data_dir = 'data'
        self.location_dir = 'location'

        self.data_file = 'data.avro'
        self.location_file = 'locations.json'

        self.source_path = os.path.join(self.input_path, 'prt', self.metadata_path)
        self.group_path = os.path.join(self.input_path, 'dualfan', self.metadata_path)

        #  source files
        self.fs.create_file(os.path.join(self.source_path, self.data_dir, self.data_file))
        self.fs.create_file(os.path.join(self.source_path, self.location_dir, self.location_file))

        #  group files
        self.fs.create_file(os.path.join(self.group_path, self.data_dir, self.data_file))
        self.fs.create_file(os.path.join(self.group_path, self.location_dir, self.location_file))

    def test_group_source(self):
        app.group_source(self.source_path, self.output_path)
        root_output_path = os.path.join(self.output_path, 'prt', self.metadata_path)
        source_data_path = os.path.join(root_output_path, self.data_dir, self.data_file)
        source_location_path = os.path.join(root_output_path, self.location_dir, self.location_file)
        self.assertTrue(os.path.lexists(source_data_path))
        self.assertTrue(os.path.lexists(source_location_path))

    def test_main(self):
        os.environ['SOURCE_PATH'] = self.source_path
        os.environ['RELATED_INPUTS'] = self.group_path
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        root_output_path = os.path.join(self.output_path, 'prt', self.metadata_path)
        source_data_path = os.path.join(root_output_path, self.data_dir, self.data_file)
        print(f'test_source_data_path: {source_data_path}')
        source_location_path = os.path.join(root_output_path, self.location_dir, self.location_file)
        print(f'test_source_location_path: {source_location_path}')
        related_path = os.path.join(root_output_path, 'related_locations', 'dualfan')
        group_data_path = os.path.join(related_path, self.metadata_path, self.data_dir, self.data_file)
        print(f'test_group_data_path: {group_data_path}')
        group_location_path = os.path.join(related_path, self.metadata_path, self.location_dir, self.location_file)
        print(f'test_group_location_path: {group_location_path}')
        self.assertTrue(os.path.lexists(source_data_path))
        self.assertTrue(os.path.lexists(source_location_path))
        self.assertTrue(os.path.lexists(group_data_path))
        self.assertTrue(os.path.lexists(group_location_path))


if __name__ == '__main__':
    unittest.main()
