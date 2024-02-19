#!/usr/bin/env python3
import os
from collections import defaultdict
import unittest

from pachyderm_sdk import Client

from errored_datums_reader import reader_main
from errored_datums_reader.reader import read_error_files


class ReaderTest(unittest.TestCase):

    @staticmethod
    @unittest.skip('Requires Pachyderm client configuration.')
    def test_read_error_files() -> None:
        client = Client.from_config()
        files_by_pipeline: defaultdict[str, list[str]] = read_error_files(client)
        for pipeline_name in files_by_pipeline.keys():
            print(f'{pipeline_name} error_count: {len(files_by_pipeline[pipeline_name])}')

    @staticmethod
    @unittest.skip('Integration test skipped due to long process time.')
    def test_main() -> None:
        os.environ['DB_CONFIG_SOURCE'] = 'iam'  # Use IAM authentication after 'gcloud auth login'.
        os.environ['CLIENT_SOURCE'] = 'config'  # Use local ~/.pachyderm/config.json.
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['AUTHORIZATION_TOKEN'] = ''  # Not needed on local machine after 'pachctl auth login'.
        reader_main.main()
