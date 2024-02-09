#!/usr/bin/env python3
import os
from collections import defaultdict
import unittest

from data_access.tests.database_test import DatabaseBackedTest
from error_file_reader import reader_main
from error_file_reader.reader import read_error_files
from error_file_reader.reader_main import get_local_client


class ReaderTest(DatabaseBackedTest):

    def setUp(self) -> None:
        # self.setUpPyfakefs()
        # self.out_path = Path('/out')
        # self.fs.create_dir(self.out_path)
        pass

    @staticmethod
    def test_read_error_files() -> None:
        client = get_local_client()
        paths_by_repo: defaultdict[str, list[str]] = read_error_files(client)
        for key in paths_by_repo.keys():
            print(f'repo: {key} error file count: {len(paths_by_repo[key])}')

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self) -> None:
        self.configure_mount()
        os.environ['DB_CONFIG_SOURCE'] = 'environment'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        reader_main.main()
