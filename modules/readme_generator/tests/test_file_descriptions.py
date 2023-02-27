#!/usr/bin/env python3
import unittest
from typing import Dict

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from readme_generator.file_descriptions import get_descriptions


class FileDescriptionsTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_descriptions(self):
        self.configure_mount()
        dp_config = read_from_environment()
        descriptions: Dict[str, str] = get_descriptions(DbConnector(dp_config))
        for key in descriptions:
            print(f'key: {key} value: {descriptions.get(key)}')
