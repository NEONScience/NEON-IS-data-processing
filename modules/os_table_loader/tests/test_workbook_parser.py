#!/usr/bin/env python3
import unittest

from os_table_loader.publication.workbook_parser import filter_workbook_rows


class WorkbookParserTest(unittest.TestCase):

    def test_filter_workbook_rows_expanded_includes_basic_and_expanded(self):
        rows = [
            {'table': 'my_table', 'downloadPkg': 'basic', 'fieldName': 'field_basic'},
            {'table': 'my_table', 'downloadPkg': 'expanded', 'fieldName': 'field_expanded'},
            {'table': 'my_table', 'downloadPkg': 'none', 'fieldName': 'field_none'},
            {'table': 'other_table', 'downloadPkg': 'basic', 'fieldName': 'other_field'},
        ]

        filtered_rows = filter_workbook_rows(rows, 'my_table', 'expanded')

        field_names = [row['fieldName'] for row in filtered_rows]
        self.assertEqual(field_names, ['field_basic', 'field_expanded'])

    def test_filter_workbook_rows_basic_only_includes_basic(self):
        rows = [
            {'table': 'my_table', 'downloadPkg': 'basic', 'fieldName': 'field_basic'},
            {'table': 'my_table', 'downloadPkg': 'expanded', 'fieldName': 'field_expanded'},
        ]

        filtered_rows = filter_workbook_rows(rows, 'my_table', 'basic')

        field_names = [row['fieldName'] for row in filtered_rows]
        self.assertEqual(field_names, ['field_basic'])


if __name__ == '__main__':
    unittest.main()
