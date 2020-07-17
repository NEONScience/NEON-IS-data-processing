#!/usr/bin/env python3
import os
from pathlib import Path

import unittest
from pyfakefs.fake_filesystem_unittest import TestCase

import joiner.joiner_main as joiner_main
from joiner.joiner import join_files


class JoinerTest(TestCase):

    def setUp(self):
        """Create data and location files for prt, dual_fan, and heater in fake filesystem."""
        self.setUpPyfakefs()
        self.input_path = Path('/repo/inputs')
        self.output_path = Path('/outputs')
        self.prt_location_1 = 'CFGLOC123'
        self.prt_location_2 = 'CFGLOC321'
        self.dual_fan_location = 'CFGLOC789'
        self.heater_location = 'CFGLOC456'
        self.metadata_path = Path('2019/05/24/aspirated-single-121')

        self.data_dir = 'data'
        self.location_dir = 'location'
        self.data_file = 'data.ext'
        self.location_file = 'locations.json'

        self.prt_path = Path('prt', self.metadata_path)
        self.dual_fan_path = Path('dual_fan', self.metadata_path)
        self.heater_path = Path('heater', self.metadata_path)
        #  prt 1
        self.prt_data_file_1 = Path(self.prt_path, self.prt_location_1, self.data_dir, self.data_file)
        self.prt_location_file_1 = Path(self.prt_path, self.prt_location_1, self.location_dir, self.location_file)
        self.fs.create_file(Path(self.input_path, self.prt_data_file_1))
        self.fs.create_file(Path(self.input_path, self.prt_location_file_1))
        # prt 2
        self.prt_data_file_2 = Path(self.prt_path, self.prt_location_2, self.data_dir, self.data_file)
        self.prt_location_file_2 = Path(self.prt_path, self.prt_location_2, self.location_dir, self.location_file)
        self.fs.create_file(Path(self.input_path, self.prt_data_file_2))
        self.fs.create_file(Path(self.input_path, self.prt_location_file_2))
        #  dualfan
        self.dualfan_data_file = Path(self.dual_fan_path, self.dual_fan_location, self.data_dir, self.data_file)
        self.dualfan_location_file = Path(self.dual_fan_path, self.dual_fan_location, self.location_dir,
                                          self.location_file)
        self.fs.create_file(Path(self.input_path, self.dualfan_data_file))
        self.fs.create_file(Path(self.input_path, self.dualfan_location_file))
        #  heater
        self.heater_data_file = Path(self.heater_path, self.heater_location, self.data_dir, self.data_file)
        self.heater_location_file = Path(self.heater_path, self.heater_location, self.location_dir, self.location_file)
        self.fs.create_file(Path(self.input_path, self.heater_data_file))
        self.fs.create_file(Path(self.input_path, self.heater_location_file))
        # Set keys and paths in the environment.
        self.related_paths = 'PRT_PATH' + ',' + 'PRT_2_PATH' + ',' + 'DUAL_FAN_PATH' + ',' + 'HEATER_PATH'
        os.environ['PRT_PATH'] = str(Path(self.input_path, self.prt_path))
        os.environ['PRT_2_PATH'] = str(Path(self.input_path, self.prt_path))
        os.environ['DUAL_FAN_PATH'] = str(Path(self.input_path, self.dual_fan_path))
        os.environ['HEATER_PATH'] = str(Path(self.input_path, self.heater_path))

        self.relative_path_index = 3

    def test_joiner(self):
        related_paths = [
            Path(self.input_path, self.prt_path),
            Path(self.input_path, self.prt_path),
            Path(self.input_path, self.dual_fan_path),
            Path(self.input_path, self.heater_path)
        ]
        join_files(related_paths=related_paths,
                   out_path=self.output_path,
                   relative_path_index=self.relative_path_index)
        self.check_output()

    def test_main(self):
        os.environ['RELATED_PATHS'] = self.related_paths
        os.environ['OUT_PATH'] = str(self.output_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        joiner_main.main()
        self.check_output()

    def check_output(self):
        prt_data_path = Path(self.output_path, self.prt_data_file_1)
        prt_location_path = Path(self.output_path, self.prt_location_file_1)

        prt_data_path_2 = Path(self.output_path, self.prt_data_file_2)
        prt_location_path_2 = Path(self.output_path, self.prt_location_file_2)

        dual_fan_path = Path(self.output_path, self.dualfan_data_file)
        dual_fan_location_path = Path(self.output_path, self.dualfan_location_file)

        heater_path = Path(self.output_path, self.heater_data_file)
        heater_location_path = Path(self.output_path, self.heater_location_file)

        self.assertTrue(prt_data_path.exists())
        self.assertTrue(prt_location_path.exists())
        self.assertTrue(prt_data_path_2.exists())
        self.assertTrue(prt_location_path_2.exists())
        self.assertTrue(dual_fan_path.exists())
        self.assertTrue(dual_fan_location_path.exists())
        self.assertTrue(heater_path.exists())
        self.assertTrue(heater_location_path.exists())
