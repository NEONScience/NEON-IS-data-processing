#!/usr/bin/env python3
from pathlib import Path
import os

from pyfakefs.fake_filesystem_unittest import TestCase

from structlog import get_logger
from egress.egress.egress import Egress
import common.log_config as log_config


class EgressTest(TestCase):

    def setUp(self):
        """Set required files in mock filesystem."""
        self.setUpPyfakefs()
        self.log = get_logger()
        log_config.configure('DEBUG')
        self.out_dir = Path('/tmp/out')
        input_root = Path('/tmp/in')
        site_month = Path('CPER/2019/01')
        self.input_dir = Path(input_root, site_month)
        # Data file
        self.source_file_name = 'NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-01.basic.20210720T001022Z.csv'
        self.target_file_name = 'NEON.DOM.SITE.DP1.00041.001/CPER/20190101T000000--20190201T000000/basic/NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-01.basic.20210720T001022Z.csv'
        data_path = Path(input_root, self.input_dir, self.source_file_name)
        self.fs.create_file(data_path)

    def test_egress(self):
        os.environ["CUSTOM_ENDPOINT"] = "endpoint"
        os.environ["AMAZON_BUCKET"] = "bucket"
        egress = Egress(self.input_dir, self.out_dir, 'egressurl')
        egress.upload()
        self.check_output()

    def check_output(self):
        """Check data files are in the output directory."""
        output_path = Path(self.out_dir, self.target_file_name)
        self.assertTrue(output_path.exists())
