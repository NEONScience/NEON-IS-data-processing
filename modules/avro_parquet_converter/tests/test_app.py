import os

from pyfakefs.fake_filesystem_unittest import TestCase

import avro_parquet_converter.app as app
from lib import log_config as log_config


class AppTest(TestCase):

    def setUp(self):
        log_config.configure('DEBUG')

        self.setUpPyfakefs()

        self.out_path = os.path.join('/', 'repo', 'outputs')
        self.in_path = os.path.join('/', 'repo', 'inputs')

        self.metadata_path = os.path.join('prt', '2019', '01', '05', '767')
        inputs_path = os.path.join(self.in_path, self.metadata_path)

        self.data_filename = 'prt_767_2019-01-05.avro'
        data_path = os.path.join(inputs_path, self.data_filename)

        # use real data file to convert
        actual_data_file_path = os.path.join(os.path.dirname(__file__), self.data_filename)
        self.fs.add_real_file(actual_data_file_path, target_path=data_path)

    def test_main(self):
        os.environ['IN_PATH'] = self.in_path
        os.environ['OUT_PATH'] = self.out_path
        os.environ['LOG_LEVEL'] = 'DEBUG'
        app.main()
        self.check_output()

    def check_output(self):
        data_path = os.path.join(self.out_path, self.metadata_path, 'prt_767_2019-01-05.gz.parquet')
        self.assertTrue(os.path.exists(data_path))

        import pandas
        with open(data_path, 'rb') as file:
            data_frame = pandas.read_parquet(file, engine='pyarrow')
            self.assertTrue((86400, 4) == data_frame.shape)
