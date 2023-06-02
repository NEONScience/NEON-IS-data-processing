#!/usr/bin/env python3
import unittest

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from pub_files.database.geolocations import get_geolocations


class SensorLocationTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_sensor_locations(self):
        self.configure_mount()
        db_config = read_from_environment()
        named_location = 'CFGLOC101777'
        connector = DbConnector(db_config)
        geolocations = get_geolocations(connector, named_location)
        geolocation = geolocations[0]
        print(f'geolocation:\n\n{geolocation}')
        assert geolocation.alpha == 2
        assert geolocation.beta == 0
        assert geolocation.gamma == 300
        assert len(geolocations) == 1
        assert geolocation.offset_description == 'Central Plains Soil Plot, SP2'
