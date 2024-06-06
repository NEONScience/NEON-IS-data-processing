#!/usr/bin/env python3
import unittest
from collections import defaultdict

from pachyderm_sdk import Client

from errored_datums_reader import app
from errored_datums_reader.reader import read_error_files
from errored_datums_reader.tests import db_connector_iam
from errored_datums_reader.tests.db_connector_iam import read_environment


@unittest.skip('Integration test.')
class ReaderTest(unittest.TestCase):

    def setUp(self):
        self.client = Client.from_config()
        self.db = db_connector_iam.connect(read_environment())

    @unittest.skip('Requires Pachyderm client configuration.')
    def test_read_error_files(self) -> None:
        files_by_pipeline: defaultdict[str, list[str]] = read_error_files(self.client)
        for pipeline_name in files_by_pipeline.keys():
            print(f'{pipeline_name} error_count: {len(files_by_pipeline[pipeline_name])}')

    @unittest.skip('Integration test skipped due to long process time.')
    def test_app(self) -> None:
        app.run(self.client, self.db)
