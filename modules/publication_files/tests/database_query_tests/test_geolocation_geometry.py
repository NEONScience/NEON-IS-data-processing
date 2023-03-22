#!/usr/bin/env python3
import unittest

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from publication_files.database_queries.geolocation_geometry import get_geometry, format_coordinates


class LocationGeometryTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_geometry(self):
        self.configure_mount()
        db_config = read_from_environment()
        site = 'CPER'
        geometry = get_geometry(DbConnector(db_config), site)
        coordinates = format_coordinates(geometry)
        assert geometry == 'POINT Z (-104.745591 40.815536 1653.9151)'
        assert coordinates == '40.815536 -104.745591 WGS 84'
