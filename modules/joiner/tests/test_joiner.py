#!/usr/bin/env python3
import os

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

import joiner.app as app


class JoinerTest(TestCase):

    def setUp(self):
        """Create data and location files for prt, dual_fan, and heater in fake filesystem."""

        self.setUpPyfakefs()

        self.input_path = os.path.join('/', 'repo', 'inputs')
        self.output_path = os.path.join('/', 'outputs')

        self.prt_location_1 = 'CFGLOC123'
        self.prt_location_2 = 'CFGLOC321'
        self.dual_fan_location = 'CFGLOC789'
        self.heater_location = 'CFGLOC456'

        self.metadata_path = os.path.join('2019', '05', '24', 'aspirated-single-121')

        self.data_dir = 'data'
        self.location_dir = 'location'
        self.data_file = 'data.ext'
        self.location_file = 'locations.json'

        self.prt_path = os.path.join('prt', self.metadata_path)
        self.dual_fan_path = os.path.join('dual_fan', self.metadata_path)
        self.heater_path = os.path.join('heater', self.metadata_path)

        #  prt 1
        self.prt_data_file_1 = os.path.join(self.prt_path, self.prt_location_1, self.data_dir, self.data_file)
        self.prt_location_file_1 = os.path.join(self.prt_path, self.prt_location_1, self.location_dir,
                                                self.location_file)

        self.fs.create_file(os.path.join(self.input_path, self.prt_data_file_1))
        self.fs.create_file(os.path.join(self.input_path, self.prt_location_file_1))

        # prt 2
        self.prt_data_file_2 = os.path.join(self.prt_path, self.prt_location_2, self.data_dir, self.data_file)
        self.prt_location_file_2 = os.path.join(self.prt_path, self.prt_location_2, self.location_dir,
                                                self.location_file)

        self.fs.create_file(os.path.join(self.input_path, self.prt_data_file_2))
        self.fs.create_file(os.path.join(self.input_path, self.prt_location_file_2))

        #  dualfan
        self.dualfan_data_file = os.path.join(self.dual_fan_path, self.dual_fan_location, self.data_dir, self.data_file)
        self.dualfan_location_file = os.path.join(self.dual_fan_path, self.dual_fan_location, self.location_dir,
                                                  self.location_file)

        self.fs.create_file(os.path.join(self.input_path, self.dualfan_data_file))
        self.fs.create_file(os.path.join(self.input_path, self.dualfan_location_file))

        #  heater
        self.heater_data_file = os.path.join(self.heater_path, self.heater_location, self.data_dir, self.data_file)
        self.heater_location_file = os.path.join(self.heater_path, self.heater_location, self.location_dir,
                                                 self.location_file)

        self.fs.create_file(os.path.join(self.input_path, self.heater_data_file))
        self.fs.create_file(os.path.join(self.input_path, self.heater_location_file))

        # Set keys and paths in the environment.
        self.related_paths = 'PRT_PATH' + ',' + 'PRT_2_PATH' + ',' + 'DUAL_FAN_PATH' + ',' + 'HEATER_PATH'
        os.environ['PRT_PATH'] = os.path.join(self.input_path, self.prt_path)
        os.environ['PRT_2_PATH'] = os.path.join(self.input_path, self.prt_path)
        os.environ['DUAL_FAN_PATH'] = os.path.join(self.input_path, self.dual_fan_path)
        os.environ['HEATER_PATH'] = os.path.join(self.input_path, self.heater_path)

        self.relative_path_index = 3

    def test_main(self):
        os.environ['RELATED_PATHS'] = self.related_paths
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        app.main()
        self.check_output()

    def check_output(self):
        prt_data_path = os.path.join(self.output_path, self.prt_data_file_1)
        prt_location_path = os.path.join(self.output_path, self.prt_location_file_1)
        print(f'prt_data_path: {prt_data_path}')
        print(f'prt_location_path: {prt_location_path}')

        prt_data_path_2 = os.path.join(self.output_path, self.prt_data_file_2)
        prt_location_path_2 = os.path.join(self.output_path, self.prt_location_file_2)
        print(f'prt_data_path_2: {prt_data_path_2}')
        print(f'prt_location_path_2: {prt_location_path_2}')

        dual_fan_path = os.path.join(self.output_path, self.dualfan_data_file)
        dual_fan_location_path = os.path.join(self.output_path, self.dualfan_location_file)
        print(f'dual_fan_data_path: {dual_fan_path}')
        print(f'dual_fan_location_path: {dual_fan_location_path}')

        heater_path = os.path.join(self.output_path, self.heater_data_file)
        heater_location_path = os.path.join(self.output_path, self.heater_location_file)
        print(f'heater_data_path: {heater_path}')
        print(f'heater_location_path: {heater_location_path}')

        self.assertTrue(os.path.lexists(prt_data_path))
        self.assertTrue(os.path.lexists(prt_location_path))
        self.assertTrue(os.path.lexists(prt_data_path_2))
        self.assertTrue(os.path.lexists(prt_location_path_2))
        self.assertTrue(os.path.lexists(dual_fan_path))
        self.assertTrue(os.path.lexists(dual_fan_location_path))
        self.assertTrue(os.path.lexists(heater_path))
        self.assertTrue(os.path.lexists(heater_location_path))


if __name__ == '__main__':
    unittest.main()
