#!usr/bin/env python3
import os
from structlog import get_logger
import common.log_config as log_config
from pathlib import Path
from unittest import TestCase
from replace_macaddress_with_assetuid import load_assetuid

log = get_logger()


class LoadAssetuidTest(TestCase):

    def setUp(self):
        self.data_path = 'pfs/data_path'
        self.map_path = 'pfs/map_path'
        self.out_path = 'pfs/out_path'
        log_level: str = 'debug'
        log_config.configure(log_level)
        log.debug(f'out_path: {self.out_path}')
        print(f'final output path is: {Path(self.out_path)}')
    #
    def test_load_assetuid(self):
        load_assetuid()

if __name__ == '__main__':
    unittest()
