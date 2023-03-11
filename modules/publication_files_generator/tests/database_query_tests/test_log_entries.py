#!/usr/bin/env python3
import unittest

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from publication_files_generator.database_queries.log_entries import get_log_entries, LogEntry


class LogEntryTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_log_entries(self):
        self.configure_mount()
        dp_config = read_from_environment()
        data_product_id = 'NEON.DOM.SITE.DP1.00001.001'
        log_entries = get_log_entries(DbConnector(dp_config), data_product_id)
        entry: LogEntry = log_entries[0]
        assert len(log_entries) == 92
        assert entry.issue == '2D sensor transducer cap became dislodged rendering calibrations invalid.'
        assert entry.resolution == 'Sensor swapped. Affected data has had manual flagging applied.'
