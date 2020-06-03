#!/usr/bin/env python3
import os

from pyfakefs.fake_filesystem_unittest import TestCase

from dag.pipeline_specification_parser import PipelineSpecificationParser


class ParserTest(TestCase):

    def setUp(self):
        """Create files to parse in fake filesystem."""
        self.setUpPyfakefs()

        self.yaml_root = os.path.join('/', 'pipe', 'yaml')
        self.json_root = os.path.join('/', 'pipe', 'json')
        self.yaml_path = os.path.join(self.yaml_root, 'pipeline.yaml')
        self.json_path = os.path.join(self.json_root, 'pipeline.json')

        config_file_path = os.path.join(os.path.dirname(__file__), 'pipeline.yaml')
        self.fs.add_real_file(config_file_path, target_path=self.yaml_path)

        config_file_path = os.path.join(os.path.dirname(__file__), 'pipeline.json')
        self.fs.add_real_file(config_file_path, target_path=self.json_path)

    def test_parse_yaml(self):
        parser = PipelineSpecificationParser(self.yaml_path, self.yaml_root)
        self.assertTrue(len(parser.get_pipeline_files()) == 1)

    def test_parse_json(self):
        parser = PipelineSpecificationParser(self.json_path, self.json_root)
        self.assertTrue(len(parser.get_pipeline_files()) == 1)
