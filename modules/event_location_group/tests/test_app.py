#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import event_location_group.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):

        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.source_id = '00001'

        self.out_path = Path('/', 'repo', 'outputs')
        self.data_path = Path('/', 'repo', 'events', 'heater', '2019', '01', '01', self.source_id)
        self.location_path = Path('/', 'location')

        #  Create data file.
        self.data_file = f'heater_{self.source_id}_events_2019-01-01.json'
        self.input_data_path = Path(self.data_path, self.data_file)
        self.fs.create_file(self.input_data_path)

        #  Create location file.
        self.location_file = f'heater_{self.source_id}_locations.json'
        self.input_location_path = Path(self.location_path, 'heater', self.source_id, self.location_file)
        self.fs.create_file(self.input_location_path)

        #  Create output directory.
        self.fs.create_dir(self.out_path)

        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7
        self.filename_index = 8

    def test_group(self):
        app.group(self.data_path,
                  self.location_path,
                  self.out_path,
                  self.source_type_index,
                  self.year_index,
                  self.month_index,
                  self.day_index,
                  self.source_id_index,
                  self.filename_index)
        self.check_output()

    def test_main(self):
        os.environ['DATA_PATH'] = str(self.data_path)
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['FILENAME_INDEX'] = str(self.filename_index)

        app.main()
        self.check_output()

    def check_output(self):
        print(f'input_data_path: {self.input_data_path}')
        print(f'input_location_path: {self.input_location_path}')

        root_path = Path(self.out_path, 'heater', '2019', '01', '01', '00001')

        output_data_path = Path(root_path, 'data', self.data_file)
        output_location_path = Path(root_path, 'location', self.location_file)

        print(f'output_data_path: {output_data_path}')
        print(f'output_location_path: {output_location_path}')

        self.assertTrue(output_data_path.exists())
        self.assertTrue(output_location_path.exists())
