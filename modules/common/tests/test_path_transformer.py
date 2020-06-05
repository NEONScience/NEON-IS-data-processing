#!/usr/bin/env python3
from pathlib import Path

from pyfakefs import fake_filesystem_unittest

from common.path_transformer import PathTransformer


class PathTransformerTest(fake_filesystem_unittest.TestCase):

    def setUp(self):
        self.setUpPyfakefs()

    def test_transform_int(self):
        input_path = Path('/test/input')
        output_path = Path('/test/output')
        file_path = input_path.joinpath('stuff/file.foo')
        self.fs.create_file(file_path)
        expected_path = Path('/test/output/stuff/file.foo')
        relative_path_index = 3
        transformer = PathTransformer(input_path, output_path)
        transformer.transform(relative_path_index)
        self.assertTrue(expected_path.exists())

    def test_transform_list(self):
        input_path = Path('/test2/input')
        output_path = Path('/test2/output')
        file_path = input_path.joinpath('stuff/file.foo')
        self.fs.create_dir(output_path)
        self.fs.create_file(file_path)
        expected_path = output_path.joinpath('stuff/file.foo')
        stuff_dir_index = 3
        filename_index = 4
        indices = [stuff_dir_index, filename_index]
        transformer = PathTransformer(input_path, output_path)
        transformer.transform(indices)
        self.assertTrue(expected_path.exists())
