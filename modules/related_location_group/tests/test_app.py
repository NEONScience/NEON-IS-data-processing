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

        self.group = 'aspirated-single-121'

        self.prt_location_1 = 'CFGLOC123'
        self.prt_location_2 = 'CFGLOC321'
        self.dualfan_location = 'CFGLOC789'
        self.heater_location = 'CFGLOC456'

        self.metadata_path = os.path.join('2019', '05', '24', self.group)

        self.data_dir = 'data'
        self.location_dir = 'location'

        self.data_file = 'data.avro'
        self.location_file = 'locations.json'

        self.prt_path = os.path.join(self.input_path, 'prt', self.metadata_path)
        self.dualfan_path = os.path.join(self.input_path, 'dualfan', self.metadata_path)
        self.heater_path = os.path.join(self.input_path, 'heater', self.metadata_path)

        #  prt 1
        self.fs.create_file(os.path.join(self.prt_path, self.prt_location_1, self.data_dir, self.data_file))
        self.fs.create_file(os.path.join(self.prt_path, self.prt_location_1, self.location_dir, self.location_file))

        # prt 2
        self.fs.create_file(os.path.join(self.prt_path, self.prt_location_2, self.data_dir, self.data_file))
        self.fs.create_file(os.path.join(self.prt_path, self.prt_location_2, self.location_dir, self.location_file))

        #  dualfan
        self.fs.create_file(os.path.join(self.dualfan_path, self.dualfan_location, self.data_dir, self.data_file))
        self.fs.create_file(os.path.join(self.dualfan_path, self.dualfan_location, self.location_dir,
                                         self.location_file))

        #  heater
        self.fs.create_file(os.path.join(self.heater_path, self.heater_location, self.data_dir, self.data_file))
        self.fs.create_file(os.path.join(self.heater_path, self.heater_location, self.location_dir, self.location_file))

        # Set keys and paths in the environment.
        self.related_paths = 'PRT_PATH' + ',' + 'PRT_2_PATH' + ',' + 'DUALFAN_PATH' + ',' + 'HEATER_PATH'
        os.environ['PRT_PATH'] = self.prt_path
        os.environ['PRT_2_PATH'] = self.prt_path
        os.environ['DUALFAN_PATH'] = self.dualfan_path
        os.environ['HEATER_PATH'] = self.heater_path

    def test_group_related(self):
        app.group_related(self.related_paths, self.output_path)
        self.check_output()

    def test_main(self):
        os.environ['RELATED_PATHS'] = self.related_paths
        os.environ['OUT_PATH'] = self.output_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        root_path = os.path.join(self.output_path, self.metadata_path)

        prt_data_path = os.path.join(root_path, 'prt', self.prt_location_1, self.data_dir, self.data_file)
        prt_location_path = os.path.join(root_path, 'prt', self.prt_location_1, self.location_dir, self.location_file)
        print(f'prt_data_path: {prt_data_path}')
        print(f'prt_location_path: {prt_location_path}')

        prt_data_path_2 = os.path.join(root_path, 'prt', self.prt_location_2, self.data_dir, self.data_file)
        prt_location_path_2 = os.path.join(root_path, 'prt', self.prt_location_2, self.location_dir, self.location_file)
        print(f'prt_data_path_2: {prt_data_path_2}')
        print(f'prt_location_path_2: {prt_location_path_2}')

        dualfan_root = os.path.join(root_path, 'dualfan', self.dualfan_location)
        dualfan_path = os.path.join(dualfan_root, self.data_dir, self.data_file)
        dualfan_location_path = os.path.join(dualfan_root, self.location_dir, self.location_file)
        print(f'dualfan_data_path: {dualfan_path}')
        print(f'dualfan_location_path: {dualfan_location_path}')

        heater_root = os.path.join(root_path, 'heater', self.heater_location)
        heater_path = os.path.join(heater_root, self.data_dir, self.data_file)
        heater_location_path = os.path.join(heater_root, self.location_dir, self.location_file)
        print(f'heater_data_path: {heater_path}')
        print(f'heater_location_path: {heater_location_path}')

        self.assertTrue(os.path.lexists(prt_data_path))
        self.assertTrue(os.path.lexists(prt_location_path))
        self.assertTrue(os.path.lexists(prt_data_path_2))
        self.assertTrue(os.path.lexists(prt_location_path_2))
        self.assertTrue(os.path.lexists(dualfan_path))
        self.assertTrue(os.path.lexists(dualfan_location_path))
        self.assertTrue(os.path.lexists(heater_path))
        self.assertTrue(os.path.lexists(heater_location_path))


if __name__ == '__main__':
    unittest.main()
