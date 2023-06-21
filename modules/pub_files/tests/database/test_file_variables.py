#!/usr/bin/env python3
import unittest
from typing import List, Callable

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from pub_files.database.file_variables import make_get_sensor_position_variables, FileVariables


class FileVariablesTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_sensor_positions(self):
        self.configure_mount()
        db_config = read_from_environment()
        connector = DbConnector(db_config)
        get_sensor_position_variables: Callable[[], List[FileVariables]] = make_get_sensor_position_variables(connector)
        variables: List[FileVariables] = get_sensor_position_variables()
        print(variables)
        term_name = variables[0].term_name
        assert term_name == 'HOR.VER'
