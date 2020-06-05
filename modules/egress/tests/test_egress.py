#!/usr/bin/env python3
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from egress.egress.egress import Egress
import common.log_config as log_config


class EgressTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""

        log_config.configure('DEBUG')

        self.out_dir = Path('/', 'tmp', 'outputs')
        self.location = 'CFGLOC112154'
        input_root = Path('/', 'tmp', 'inputs')

        source_month = Path('prt', '2019', '01')
        self.input_data_dir = Path(input_root, source_month, '03')
        self.source_dir = Path(source_month, '03', self.location)

        self.out_name = 'outname'
        self.target_date = '2019-01-03'
        self.date_index = 2
        self.loc_index = 1

        self.setUpPyfakefs()

        self.data_dir = 'data'

        # Data file
        self.source_file_name = 'prt_CFGLOC112154_2019-01-03_basicStats_030.ext'
        self.target_file_name = 'outname_2019-01-03_CFGLOC112154_basicStats_030.ext'
        data_path = Path(input_root, self.source_dir, self.data_dir, self.source_file_name)
        self.fs.create_file(data_path)

    def test_egress(self):
        egress = Egress(self.input_data_dir, self.out_dir, self.out_name, self.date_index, self.loc_index)
        egress.upload()
        self.check_output()

    def check_output(self):
        """Check data files are in the output directory."""
        output_path = Path(self.out_dir, self.out_name, self.target_date, self.location, self.target_file_name)
        self.assertTrue(output_path.exists())
