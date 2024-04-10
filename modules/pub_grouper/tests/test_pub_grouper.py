#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import pub_grouper.pub_grouper_main as pub_grouper_main
import pub_grouper.tests.group_json as group_json
from pub_grouper.pub_grouper import pub_group


class PubGrouperTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.input_path = Path('/repo/inputs')
        self.output_path = Path('/outputs')
        self.err_path = Path('errored')
        self.group = "par-quantum-line_CPER001000"
        self.site = "CPER"
        self.date_path = Path("2019/05/24")
        self.data_path = Path(self.input_path, self.date_path, self.group, 'data')
        self.fs.create_dir(self.data_path)
        self.relative_path_index = 3
        self.group_metadata_dir = 'group'
        self.group_path = Path(self.input_path, self.date_path, self.group, self.group_metadata_dir)
        self.fs.create_dir(self.group_path)
        self.data_file = self.group + "_data.ext"
        self.group_file = "CFGLOCXXXXXX.json"
        self.base_path = Path(self.input_path, self.date_path)
        self.in_data_path = Path(self.data_path, self.data_file)
        self.in_group_path = Path(self.group_path, self.group_file)
        self.year_index = self.relative_path_index
        self.group_index = self.relative_path_index + 3
        self.data_type_index = self.relative_path_index + 4
        self.publoc_key = 'site'
        self.symlink = True
        self.in_data_path.touch()
        self.in_group_path.write_text(group_json.get_group_json())
        self.product = group_json.get_data_product()

    def test_pub_group(self):
        pub_group(data_path=self.input_path,
                  out_path=self.output_path,
                  err_path = self.err_path,
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
        os.environ["ERR_PATH"] = str(self.err_path)
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
