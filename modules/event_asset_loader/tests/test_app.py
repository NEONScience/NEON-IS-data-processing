#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

import event_asset_loader.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):

        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.source_id = '0001'
        self.location_file = 'heater_' + self.source_id + '_locations.json'
        self.out_path = os.path.join('/', 'outputs', 'repo',)
        self.source_path = os.path.join('/', 'inputs', 'repo', 'heater', self.source_id, self.location_file)
        #  Create data input file.
        self.fs.create_file(self.source_path)
        #  Create output directory.
        self.fs.create_dir(self.out_path)

        self.source_type_index = 3
        self.source_id_index = 4
        self.filename_index = 5

    def test_group(self):
        app.process(self.source_path,
                    self.out_path,
                    self.source_type_index,
                    self.source_id_index,
                    self.filename_index)
        self.check_output()

    def test_main(self):
        os.environ['SOURCE_PATH'] = self.source_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['FILENAME_INDEX'] = str(self.filename_index)

        app.main()
        self.check_output()

    def check_output(self):
        print(f'source_path: {self.source_path}')
        self.output_path = os.path.join(self.out_path, 'heater', self.source_id,
                                        'heater_' + self.source_id + '_events.json')
        print(f'output_path: {self.output_path}')
        self.assertTrue(os.path.lexists(self.output_path))
