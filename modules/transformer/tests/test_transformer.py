#!/usr/bin/env python3
import os
from pathlib import Path
from unittest import TestCase
from transformer.transformer import transform
import transformer.transformer_main as transformer_main
import tempfile
import shutil
import json

class TransformerTest(TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory().name
        self.input_path = Path(self.temp_dir, "repo/inputs")
        self.output_path = Path(self.temp_dir, "outputs")
        self.location = "CFGLOC123"
        self.site = "CPER"
        self.metadata_path = Path("2019/05/24")
        self.data_path = Path(self.input_path, "prt", self.metadata_path, self.location)
        self.location_path = Path(self.input_path, "prt", self.location)
        os.makedirs(self.data_path)
        os.makedirs(self.location_path)
        self.data_file = self.location+"_data.ext"
        self.location_file = self.location+"_locations.json"
        self.base_path = Path(self.input_path, "prt", self.metadata_path)
        self.in_data_path = Path(self.data_path, self.data_file)
        self.in_location_path = Path(self.input_path, "prt", self.location, self.location_file)
        self.relative_path_index = 9
        self.year_index = 10
        self.loc_index = 13
        self.related_paths = "DATA_PATH" + "," + "LOCATION_PATH"
        os.environ["DATA_PATH"] = str(Path(self.input_path, self.data_path))
        os.environ["LOCATION_PATH"] = str(Path(self.input_path, self.location_path))

        self.in_data_path.touch()
        loc_json = {'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'geometry': None, 'properties': {'name': 'CFGLOC108605', 'type': 'CONFIG', 'description': 'Mountain Lake Soil Temp Profile SP3, Z6 Depth', 'site': 'CPER', 'context': ['soil'], 'active_periods': [{'start_date': '2017-07-20T00:00:00Z'}]}, 'Required Asset Management Location Code': 'CFGLOC108605', 'Required Asset Management Location ID': 10080, 'HOR': '003', 'VER': '506', 'TMI': '000', 'Data Rate': '0.1'}]}
        self.in_location_path.write_text(json.dumps(loc_json))

    def test_transform(self):
        related_paths = [self.data_path, self.location_path]
        transform(related_paths=related_paths,
                       data_path=self.data_path,
                       location_path=self.location_path,
                       out_path=self.output_path,
                       relative_path_index=self.relative_path_index,
                       year_index=self.year_index,
                       loc_index=self.loc_index)
        self.check_output()

    def test_main(self):
        os.environ["RELATED_PATHS"] = str(self.related_paths)
        os.environ["DATA_PATH"] = str(self.data_path)
        os.environ["LOCATION_PATH"] = str(self.location_path)
        os.environ["OUT_PATH"] = str(self.output_path)
        os.environ["LOG_LEVEL"] = "DEBUG"
        os.environ["RELATIVE_PATH_INDEX"] = str(self.relative_path_index)
        os.environ["YEAR_INDEX"] = str(self.year_index)
        os.environ["LOC_INDEX"] = str(self.loc_index)
        transformer_main.main()
        self.check_output()

    def check_output(self):
        root_path = Path(self.output_path, self.site, *self.metadata_path.parts[0:3])
        out_data_path = Path(root_path, self.data_file)
        out_location_path = Path(root_path, self.location_file)
        self.assertTrue(out_data_path.exists())
        self.assertTrue(out_location_path.exists())

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
