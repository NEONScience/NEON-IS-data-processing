import os

from pyfakefs.fake_filesystem_unittest import TestCase

import event_asset_loader.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):

        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.source_id = '0001'
        self.out_path = os.path.join('/', 'outputs', 'repo',)
        self.source_path = os.path.join('/', 'inputs', 'repo', 'heater', self.source_id,
                                        'heater_' + self.source_id + '_locations.json')
        #  Create data input file.
        self.fs.create_file(self.source_path)
        #  Create output dir
        self.fs.create_dir(self.out_path)

    def test_group(self):
        app.process(self.source_path, self.out_path)
        self.check_output()

    def test_main(self):
        os.environ['SOURCE_PATH'] = self.source_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        print(f'source_path: {self.source_path}')
        self.output_path = os.path.join(self.out_path, 'heater', self.source_id, 'heater_' + self.source_id + '.json')
        print(f'output_path: {self.output_path}')
        self.assertTrue(os.path.lexists(self.output_path))
