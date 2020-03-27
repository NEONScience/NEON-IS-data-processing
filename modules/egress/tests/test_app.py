import os

from pyfakefs.fake_filesystem_unittest import TestCase

from egress.egress.egress import Egress
import lib.log_config as log_config


class AppTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""

        log_config.configure('DEBUG')

        self.out_dir = os.path.join('/', 'tmp', 'outputs')
        self.location = 'CFGLOC112154'
        self.input_root = os.path.join('/', 'tmp', 'inputs',)

        source_month = os.path.join('prt', '2019', '01')
        self.input_data_dir = os.path.join(self.input_root, source_month, '03')
        self.source_dir = os.path.join(source_month, '03', self.location)

        self.out_name = 'outname'
        self.target_date = '2019-01-03'
        self.date_index = 2
        self.loc_index = 1

        self.setUpPyfakefs()

        self.data_dir = 'data'

        # Data file
        self.source_file_name = 'prt_CFGLOC112154_2019-01-03_basicStats_030.ext'
        self.target_file_name = 'outname_2019-01-03_CFGLOC112154_basicStats_030.ext'
        data_path = os.path.join(self.input_root, self.source_dir, self.data_dir, self.source_file_name)
        self.fs.create_file(data_path)
        print('Egress test')
        print(f'input data_path: {data_path}')

    def test_egress(self):
        egress = Egress(self.input_data_dir, self.out_dir, self.out_name, self.date_index, self.loc_index)
        egress.upload()
        self.check_output()

    def check_output(self):
        """Check data files are in the output directory."""
        target_path = os.path.join(self.out_dir, self.out_name, self.target_date, self.location, self.target_file_name)
        print(f'target_path: {target_path}')
        self.assertTrue(os.path.lexists(target_path))
