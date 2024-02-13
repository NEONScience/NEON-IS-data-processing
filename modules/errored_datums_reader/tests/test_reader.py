#!/usr/bin/env python3
import os
from collections import defaultdict
import unittest

from data_access.tests.database_test import DatabaseBackedTest
from errored_datums_reader import reader_main
from errored_datums_reader.reader import read_error_files
from errored_datums_reader.reader_main import get_local_client


class ReaderTest(DatabaseBackedTest):

    def setUp(self) -> None:
        # self.setUpPyfakefs()
        # self.out_path = Path('/out')
        # self.fs.create_dir(self.out_path)
        pass

    @staticmethod
    @unittest.skip('Requires Pachyderm client configuration.')
    def test_read_error_files() -> None:
        client = get_local_client()
        files_by_pipeline: defaultdict[str, list[str]] = read_error_files(client)
        for pipeline_name in files_by_pipeline.keys():
            print(f'pipeline: {pipeline_name} error file count: {len(files_by_pipeline[pipeline_name])}')

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self) -> None:
        self.configure_mount()
        os.environ['DB_CONFIG_SOURCE'] = 'environment'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['AUTHORIZATION_TOKEN'] = ''  # Not needed on local machine if authenticated.
        reader_main.main()
