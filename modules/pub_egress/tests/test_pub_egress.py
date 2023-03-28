#!/usr/bin/env python3
from pathlib import Path
import os

from testfixtures import TempDirectory
from unittest import TestCase
import sys

from structlog import get_logger
from pub_egress.pub_egress.pub_egress import Pub_egress
import common.log_config as log_config
import logging


class PubEgressTest(TestCase):

    def setUp(self):
        self.log = logging.getLogger('testlog')
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
        self.temp_dir = TempDirectory()  # generate_readme temporary dir
        self.temp_dir_name = self.temp_dir.path
        self.input_root = Path(self.temp_dir_name, 'repo/inputs')
        self.out_dir = Path(self.temp_dir_name, 'out')
        site_month = Path('CPER/2019/01')
        self.input_dir = Path(self.input_root, site_month)
        # Data file
        self.source_file_name = 'NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-01.basic.20210720T001022Z.csv'
        self.target_file_name = 'NEON.DOM.SITE.DP1.00041.001/CPER/20190101T000000--20190201T000000/basic/NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-01.basic.20210720T001022Z.csv'
        data_path = Path(self.input_dir, self.source_file_name)
        os.makedirs(self.input_dir)
        with open(data_path, 'w') as f:
            f.write('file 1 content')
        # Manifest file
        manifest_file = 'manifest.csv'
        manifest_path = Path(self.input_dir,manifest_file)
        with open(manifest_path, 'w') as f:
            f.write('file,hasData,visibility,size,checksum\n')
            f.write('NEON.D10.CPER.DP1.00041.001.001.501.001.ST_1_minute.2019-01.basic.20210720T001022Z.csv,True,public,94064,9964c27c73a86313a24f573f59fc2d52')


    def test_egress(self):
        os.environ["CUSTOM_ENDPOINT"] = "endpoint"
        os.environ["AMAZON_BUCKET"] = "bucket"
        egress = Pub_egress(self.input_dir, self.out_dir, 'egressurl')
        egress.upload()
        self.check_output()

    def check_output(self):
        """Check data files are in the output directory."""
        output_path = Path(self.out_dir, self.target_file_name)
        self.assertTrue(output_path.exists())