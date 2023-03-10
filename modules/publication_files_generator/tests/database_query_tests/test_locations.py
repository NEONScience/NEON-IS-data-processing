#!/usr/bin/env python3
import unittest

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from publication_files_generator.database_queries.sensor_locations import get_locations


class SensorLocationTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_sensor_locations(self):
        self.configure_mount()
        db_config = read_from_environment()
        named_location = 'CFGLOC101777'
        connector = DbConnector(db_config)
        locations = get_locations(connector, named_location)
        location = locations[0]
        assert location.alpha == 2
        assert location.beta == 0
        assert location.gamma == 300
        assert len(locations) == 1
