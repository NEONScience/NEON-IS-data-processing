#!/usr/bin/env python3
import os

import yaml
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from filter_joiner.joiner import FilterJoiner
import filter_joiner.filter_joiner_main as filter_joiner_main


class FilterJoinerStatisticsUncertaintyGroupTest(TestCase):
    """Class to simulate pipeline statistics/uncertainty joiner."""

    def setUp(self):
        """Create files to join in fake filesystem."""
        self.setUpPyfakefs()
        self.stats_path = Path('/pfs/STATISTICS_PATH')
        self.coefficient_path = Path('/pfs/UNCERTAINTY_COEF_PATH')
        self.uncertainty_data_path = Path('/pfs/UNCERTAINTY_DATA_PATH')
        self.output_path = Path('/pfs/out')
        self.location_path = Path('2019/01/01/aspirated-single-105/prt/CFGLOC123/')

        # stats files
        self.stats_path_1 = Path(self.location_path, 'data/prt_CFGLOC123_2019-01-01.parquet')
        self.fs.create_file(Path(self.stats_path, self.stats_path_1))

        # coefficient files
        self.coefficient_path_1 = Path(self.location_path, 'flags/prt_CFGLOC123_2019-01-01_flagsCal.parquet')
        self.coefficient_path_2 = Path(self.location_path, 'location/CFGLOC123.json')
        self.coefficient_path_3 = Path(self.location_path,
                                       'uncertainty_data/prt_CFGLOC123_2019-01-01_uncertainty_data.parquet')
        self.coefficient_path_4 = Path(self.coefficient_path, self.location_path, 'uncertainty_coef')
        self.fs.create_file(Path(self.coefficient_path, self.coefficient_path_1))
        self.fs.create_file(Path(self.coefficient_path, self.coefficient_path_2))
        self.fs.create_file(Path(self.coefficient_path, self.coefficient_path_3))
        self.fs.create_dir(Path(self.coefficient_path, self.coefficient_path_4))

        # uncertainty data files
        self.uncertainty_data_path_1 = Path(self.location_path,
                                            'uncertainty_data/prt_CFGLOC123_2019-01-01_uncertaintyData.parquet')
        self.fs.create_file(Path(self.uncertainty_data_path, self.uncertainty_data_path_1))

        # Use real config file
        config_file_path = Path(os.path.dirname(__file__), 'config-statistics-uncertainty-group.yaml')
        self.fs.add_real_file(config_file_path, target_path='/config.yaml')
        self.relative_path_index = 3

    def test_joiner(self):
        with open('/config.yaml') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            config = yaml.dump(data, sort_keys=True)
        filter_joiner = FilterJoiner(config=config,
                                     out_path=self.output_path,
                                     relative_path_index=self.relative_path_index)
        filter_joiner.join()
        self.check_output()

    def test_main(self):
        with open('/config.yaml') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            config = yaml.dump(data, sort_keys=True)
        os.environ['CONFIG'] = config
        os.environ['OUT_PATH'] = str(self.output_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['RELATIVE_PATH_INDEX'] = str(self.relative_path_index)
        filter_joiner_main.main()
        self.check_output()

    def check_output(self):
        coefficient_path_1 = Path(self.output_path, self.coefficient_path_1)
        coefficient_path_2 = Path(self.output_path, self.coefficient_path_2)
        coefficient_path_3 = Path(self.output_path, self.coefficient_path_3)
        coefficient_path_4 = Path(self.output_path, self.coefficient_path_4)
        stats_path = Path(self.output_path, self.stats_path_1)
        uncertainty_data_path = Path(self.output_path, self.uncertainty_data_path_1)
        self.assertFalse(coefficient_path_1.exists())
        self.assertFalse(coefficient_path_2.exists())
        self.assertFalse(coefficient_path_3.exists())
        self.assertTrue(coefficient_path_4.exists())  # only uncertainty_coef directory should pass filter
        self.assertTrue(stats_path.exists())
        self.assertTrue(uncertainty_data_path.exists())
