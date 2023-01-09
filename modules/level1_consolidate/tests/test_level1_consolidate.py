#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import level1_consolidate.level1_consolidate_main as level1_consolidate_main
from level1_consolidate.level1_consolidate_config import Config
from level1_consolidate.level1_consolidate import Level1Consolidate


class Level1ConsolidateTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.in_path = Path('/pfs/in_repo')
        self.out_path = Path('/pfs/out')
        self.relative_path_index = 3
        self.group_index = 6
        self.group_metadata_index = 7
        self.group_metadata_name = 'group'
        self.data_type_index = 9
        self.data_type_names = ['stats','quality_metrics']
        self.data_type_names_str = 'stats,quality_metrics'
        # Create test path structure
        self.group_path = Path('2019/07/23/pressure-air_HARV000025')
        # Create group metadata file
        self.group_metadata_path = Path('group')
        self.group_metadata_filename = 'CFGLOC100959.ext'
        group_metadata_path = Path(self.in_path, self.group_path, self.group_metadata_path, self.group_metadata_filename)
        self.fs.create_file(group_metadata_path)
        # Create stats file
        self.stats_path = Path('ptb330a/CFGLOC100959/stats')
        self.stats_filename = 'ptb330a_CFGLOC100959_2020-01-02_basicStats_001.ext'
        stats_path = Path(self.in_path, self.group_path, self.stats_path, self.stats_filename)
        self.fs.create_file(stats_path)
        # Create quality_metrics file - this should get consolidated into the outpu
        self.quality_metrics_path = Path('ptb330a/CFGLOC100959/quality_metrics')
        self.quality_metrics_filename = 'ptb330a_CFGLOC100959_2020-01-02_qualityMetrics_001.ext'
        quality_metrics_path = Path(self.in_path, self.group_path, self.quality_metrics_path, self.quality_metrics_filename)
        self.fs.create_file(quality_metrics_path)
        # Create dependent group data - this should not make it to the output
        self.dep_group_path = Path('rel-humidity_HARV003000/stats')
        self.dep_group_filename = 'hmp155_CFGLOC101302_2020-01-02_basicStats_001.ext'
        dep_group_path = Path(self.in_path, self.group_path, self.dep_group_path, self.dep_group_filename)
        self.fs.create_file(dep_group_path)

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        os.environ['GROUP_INDEX'] = str(self.group_index)
        os.environ['GROUP_METADATA_INDEX'] = str(self.group_metadata_index)
        os.environ['GROUP_METADATA_NAME'] = str(self.group_metadata_name)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        os.environ['DATA_TYPE_NAMES'] = self.data_type_names_str
        level1_consolidate_main.main()
        self.check_output()

    def test_consolidate_level1(self):
        config = Config(in_path=self.in_path,
                        out_path=self.out_path,
                        relative_path_index=self.relative_path_index,
                        group_index=self.group_index,
                        group_metadata_index=self.group_metadata_index,
                        group_metadata_name=self.group_metadata_name,
                        data_type_index=self.data_type_index,
                        data_type_names=self.data_type_names)
        level1_consolidate=Level1Consolidate(config)
        level1_consolidate.consolidate_paths()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.group_path)
        group_metadata_path = Path(root_path, 'group', self.group_metadata_filename)
        stats_path = Path(root_path, 'stats', self.stats_filename)
        quality_metrics_path = Path(root_path, 'quality_metrics', self.quality_metrics_filename)
        self.assertTrue(group_metadata_path.exists())
        self.assertTrue(stats_path.exists())
        self.assertTrue(quality_metrics_path.exists())
