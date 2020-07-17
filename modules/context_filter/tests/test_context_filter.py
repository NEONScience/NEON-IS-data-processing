#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from context_filter.context_filter_config import Config
from context_filter.context_filter import ContextFilter
import context_filter.context_filter_main as context_filter_main


class ContextFilterTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.out_path = Path('/out')
        self.metadata_path = Path('prt/2019/05/21/00001')
        self.context = 'aspirated-triple'  # The context to find in the location file.
        inputs_path = self.in_path.joinpath('merged', self.metadata_path)
        data_path = inputs_path.joinpath('data', 'data.ext')
        flags_path = inputs_path.joinpath('flags', 'flags.ext')
        locations_path = inputs_path.joinpath('location', 'locations.json')
        uncertainty_coefficient_path = inputs_path.joinpath('uncertainty_coefficient', 'uncertaintyCoefficient.json')
        self.fs.create_file(data_path)
        self.fs.create_file(flags_path)
        self.fs.create_file(uncertainty_coefficient_path)
        # Use real location file for parsing
        actual_location_file_path = Path(os.path.dirname(__file__), 'test-asset-location.json')
        self.fs.add_real_file(actual_location_file_path, target_path=locations_path)
        # File path indices.
        self.source_type_index = 3
        self.year_index = 4
        self.month_index = 5
        self.day_index = 6
        self.source_id_index = 7
        self.data_type_index = 8

    def test_filter_asset(self):
        config = Config(in_path=self.in_path,
                        out_path=self.out_path,
                        context=self.context,
                        source_type_index=self.source_type_index,
                        year_index=self.year_index,
                        month_index=self.month_index,
                        day_index=self.day_index,
                        source_id_index=self.source_id_index,
                        data_type_index=self.data_type_index)
        context_filter = ContextFilter(config)
        context_filter.filter_files()

    def test_main(self):
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['CONTEXT'] = self.context
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['SOURCE_TYPE_INDEX'] = str(self.source_type_index)
        os.environ['YEAR_INDEX'] = str(self.year_index)
        os.environ['MONTH_INDEX'] = str(self.month_index)
        os.environ['DAY_INDEX'] = str(self.day_index)
        os.environ['SOURCE_ID_INDEX'] = str(self.source_id_index)
        os.environ['DATA_TYPE_INDEX'] = str(self.data_type_index)
        context_filter_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.out_path, self.metadata_path)
        data_path = Path(root_path, 'data', 'data.ext')
        flags_path = Path(root_path, 'flags', 'flags.ext')
        locations_path = Path(root_path, 'location', 'locations.json')
        uncertainty_path = Path(root_path, 'uncertainty_coefficient', 'uncertaintyCoefficient.json')
        self.assertTrue(data_path.exists())
        self.assertTrue(flags_path.exists())
        self.assertTrue(locations_path.exists())
        self.assertTrue(uncertainty_path.exists())
