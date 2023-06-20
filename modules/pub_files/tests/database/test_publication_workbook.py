#!/usr/bin/env python3
import unittest

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from pub_files.database.publication_workbook import get_workbook, PublicationWorkbook


class PublicationWorkbookTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_publication_workbook(self):
        self.configure_mount()
        dp_config = read_from_environment()
        data_product_id = 'NEON.DOM.SITE.DP1.00001.001'
        publication_workbook: PublicationWorkbook = get_workbook(DbConnector(dp_config), data_product_id)
        assert publication_workbook.rows is not None
