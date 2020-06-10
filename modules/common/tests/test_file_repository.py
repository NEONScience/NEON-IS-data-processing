#!/usr/bin/env python3
from pathlib import Path

from pyfakefs import fake_filesystem_unittest

import common.file_repository as file_repository


class FileRepositoryTest(fake_filesystem_unittest.TestCase):

    def setUp(self):
        self.setUpPyfakefs()

    def test_link_index(self):
        input_path = Path('/test/input')
        output_path = Path('/test/output')
        file_path = input_path.joinpath('stuff/file.foo')
        self.fs.create_file(file_path)
        expected_path = Path('/test/output/stuff/file.foo')
        file_repository.link_index(path=input_path, output_path=output_path, path_index=3)
        self.assertTrue(expected_path.exists())

    def test_link_indices(self):
        input_path = Path('/test2/input')
        output_path = Path('/test2/output')
        file_path = input_path.joinpath('stuff/file.foo')
        self.fs.create_dir(output_path)
        self.fs.create_file(file_path)
        expected_path = output_path.joinpath('stuff/file.foo')
        directory_index = 3
        filename_index = 4
        indices = [directory_index, filename_index]
        file_repository.link_indices(path=input_path, output_path=output_path, path_indices=indices)
        self.assertTrue(expected_path.exists())
