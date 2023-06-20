#!/usr/bin/env python3
import unittest
from datetime import datetime
from typing import List, Callable

from data_access.db_config_reader import read_from_environment
from data_access.db_connector import DbConnector
from data_access.tests.database_test import DatabaseBackedTest
from pub_files.database.science_review_flags import make_get_flags, ScienceReviewFlag


class ScienceReviewFlagTest(DatabaseBackedTest):

    @unittest.skip('Integration test skipped.')
    def test_get_data_product(self):
        self.configure_mount()
        db_config = read_from_environment()
        connector = DbConnector(db_config)
        get_flags: Callable[[str, str, datetime, datetime], List[ScienceReviewFlag]] = make_get_flags(connector)
        date_format = '%Y-%m-%d'
        start_date = datetime.strptime('2000-01-01', date_format)
        end_date = datetime.strptime('2025-06-01', date_format)
        flags = get_flags('DP1.00023.001', 'OAES', start_date, end_date)
        assert len(flags) == 18
