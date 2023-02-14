#!/usr/bin/env python3
import os
from pathlib import Path
import unittest

from data_access.tests.database_test import DatabaseBackedTest
import readme_loader.readme_loader_main as readme_loader_main
from readme_loader.readme_loader import load_readme, get_filename


class ReadmeLoaderTest(DatabaseBackedTest):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)
        config_file_path = Path(os.path.dirname(__file__), get_filename())
        self.fs.add_real_file(config_file_path, target_path=f'/{get_filename()}')

    def test_load(self):

        def get_readme() -> str:  # Mock function for getting the readme template.
            with open(f'/{get_filename()}') as f:
                data = f.read()
            return data

        load_readme(get_readme, self.out_path)
        expected_path = self.out_path.joinpath(get_filename())
        self.assertTrue(expected_path.exists())

    @unittest.skip('Integration test skipped.')
    def test_main(self):
        self.configure_mount()
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        readme_loader_main.main()
        expected_path = Path(self.out_path, get_filename())
        self.assertTrue(expected_path.exists())
