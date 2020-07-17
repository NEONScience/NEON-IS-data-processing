#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from dag.pipeline_specification_parser import PipelineSpecificationParser


class ParserTest(TestCase):

    def setUp(self):
        """Create files to parse in fake filesystem."""
        self.setUpPyfakefs()
        self.yaml_root = Path('/pipe/yaml')
        self.json_root = Path('/pipe/json')
        self.yaml_path = Path(self.yaml_root, 'pipeline.yaml')
        self.json_path = Path(self.json_root, 'pipeline.json')
        config_file_path = Path(os.path.dirname(__file__), 'pipeline.yaml')
        self.fs.add_real_file(config_file_path, target_path=self.yaml_path)
        config_file_path = Path(os.path.dirname(__file__), 'pipeline.json')
        self.fs.add_real_file(config_file_path, target_path=self.json_path)

    def test_parse_yaml(self):
        parser = PipelineSpecificationParser(self.yaml_path, self.yaml_root)
        self.assertTrue(len(parser.get_pipeline_files()) == 1)

    def test_parse_json(self):
        parser = PipelineSpecificationParser(self.json_path, self.json_root)
        self.assertTrue(len(parser.get_pipeline_files()) == 1)
