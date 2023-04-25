#!/usr/bin/env python3
import unittest
from typing import List

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from pub_files.database.file_variables import get_sensor_positions, FileVariables


class FileVariablesTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_sensor_positions(self):
        self.configure_mount()
        db_config = read_from_environment()
        variables: List[FileVariables] = get_sensor_positions(DbConnector(db_config))
        print(variables)
        term_name = variables[0].term_name
        assert term_name == 'HOR.VER'
