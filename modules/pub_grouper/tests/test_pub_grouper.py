#!/usr/bin/env python3
import os
from pathlib import Path
from unittest import TestCase
from pub_grouper.pub_grouper import pub_group
import pub_grouper.pub_grouper_main as pub_grouper_main
from testfixtures import TempDirectory
import tempfile
import json

class PubGrouperTest(TestCase):

    def setUp(self):
        self.temp_dir = TempDirectory()
        self.temp_dir_name = self.temp_dir.path
        self.temp_dir_path = Path(self.temp_dir_name)
        self.temp_dir_parts = self.temp_dir_path.parts
    # offset is to ensure cross-platform Temp directory
    # Temp on Windows10 tmp on Linux and Mac
        cross_platform_offset = -1
        for dirname in self.temp_dir_parts:
            if (dirname == 'Temp') or (dirname == 'tmp') :
                break
            cross_platform_offset = cross_platform_offset + 1
        print('cross_platform_offset', cross_platform_offset)
        self.input_path = Path(self.temp_dir_name, "repo/inputs")
        self.output_path = Path(self.temp_dir_name, "outputs")
        self.group = "par-quantum-line_CPER001000"
        self.site = "CPER"
        self.date_path = Path("2019/05/24")
        self.data_path = Path(self.input_path, self.date_path, self.group,'data')
        os.makedirs(self.data_path)
        self.relative_path_index = 5 + cross_platform_offset
        self.group_metadata_dir = 'group'
        self.group_path = Path(self.input_path, self.date_path, self.group, self.group_metadata_dir)
        os.makedirs(self.group_path)
        self.data_file = self.group+"_data.ext"
        self.group_file = "CFGLOCXXXXXX.json"
        self.base_path = Path(self.input_path, self.date_path)
        self.in_data_path = Path(self.data_path, self.data_file)
        self.in_group_path = Path(self.group_path, self.group_file)
        self.year_index = self.relative_path_index
        self.group_index = self.relative_path_index + 3
        self.data_type_index = self.relative_path_index + 4
        self.publoc_key = 'site'
        self.symlink = True,
        os.environ["DATA_PATH"] = str(Path(self.data_path))

        self.in_data_path.touch()
        group_json = {'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'geometry': None, 'properties': {'name': 'CFGLOC108605', 'group': 'par-quantum-line_CPER001000', 'active_periods': [{'start_date': '2017-07-20T00:00:00Z'}], 'data_product_ID': ['DP1.00066.001']}, 'site': 'CPER', 'domain': 'D10', 'visibility_code': 'public', 'HOR': '001', 'VER': '000'}]}
        self.in_group_path.write_text(json.dumps(group_json))
        self.product = 'DP1.00066.001' # Make sure this matches the group_json
        
    def test_pub_group(self):
        pub_group(
                   data_path=self.input_path,
                   out_path=self.output_path,
                   year_index=self.year_index,
                   group_index=self.group_index,
                   data_type_index=self.data_type_index,
                   group_metadata_dir=self.group_metadata_dir,
                   publoc_key=self.publoc_key,
                   symlink=self.symlink)
        self.check_output()

    def test_main(self):
        os.environ["DATA_PATH"] = str(self.input_path)
        os.environ["OUT_PATH"] = str(self.output_path)
        os.environ["LOG_LEVEL"] = "DEBUG"
        os.environ["YEAR_INDEX"] = str(self.year_index)
        os.environ["GROUP_INDEX"] = str(self.group_index)
        os.environ["DATA_TYPE_INDEX"] = str(self.data_type_index)
        os.environ["GROUP_METADATA_DIR"] = str(self.group_metadata_dir)
        os.environ["PUBLOC_KEY"] = str(self.publoc_key)
        os.environ["LINK_TYPE"] = 'SYMLINK'
        pub_grouper_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.output_path, self.product, *self.date_path.parts[0:3], self.site)
        out_data_path = Path(root_path, 'data', self.group, self.data_file)
        out_group_path = Path(root_path, 'group', self.group, self.group_file)
        self.assertTrue(out_data_path.exists())
        self.assertTrue(out_group_path.exists())

    def tearDown(self):
        self.temp_dir.cleanup()

