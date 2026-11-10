#!/usr/bin/env python3
import unittest
from unittest.mock import MagicMock

from data_access.get_threshold_context import get_threshold_context


class GetThresholdContextTest(unittest.TestCase):
    """
    Unit test (no database required) guarding against nondeterministic context ordering.

    The underlying query has no ORDER BY (left out intentionally to avoid the DB sort
    cost per Steve's guidance), so Postgres can return context_code rows in either
    order across executions. get_threshold_context must sort app-side so the result
    is stable regardless of row order returned by the database.
    """

    @staticmethod
    def _make_connector(rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        cursor.__enter__.return_value = cursor
        cursor.__exit__.return_value = False

        connection = MagicMock()
        connection.cursor.return_value = cursor

        connector = MagicMock()
        connector.get_connection.return_value = connection
        connector.get_schema.return_value = 'test_schema'
        return connector

    def test_sorts_unordered_rows(self):
        connector = self._make_connector([('soil',), ('ion-content',)])
        self.assertEqual(get_threshold_context(connector, 'threshold-uuid'),
                          ['ion-content', 'soil'])

    def test_sorts_regardless_of_input_order(self):
        """Different row order from the DB must not change the returned order."""
        connector_a = self._make_connector([('soil',), ('ion-content',)])
        connector_b = self._make_connector([('ion-content',), ('soil',)])
        self.assertEqual(get_threshold_context(connector_a, 'threshold-uuid'),
                          get_threshold_context(connector_b, 'threshold-uuid'))

    def test_empty_result(self):
        connector = self._make_connector([])
        self.assertEqual(get_threshold_context(connector, 'threshold-uuid'), [])


if __name__ == '__main__':
    unittest.main()
