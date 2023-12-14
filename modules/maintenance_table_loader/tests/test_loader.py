#!/usr/bin/env python3
import csv
import os
import unittest
from contextlib import closing
from pathlib import Path

from pyarrow import parquet as pq

from data_access.tests.database_test import DatabaseBackedTest
from maintenance_table_loader.data_reader import DataReader
from maintenance_table_loader.loader import load_files
from maintenance_table_loader.main import main
from maintenance_table_loader.tests.mock_field_loader import get_fields
from maintenance_table_loader.tests.mock_result_values_loader import get_result_values
from maintenance_table_loader.tests.mock_result_loader import get_results
from maintenance_table_loader.tests.mock_table_loader import get_tables


class LoaderTest(DatabaseBackedTest):

    def setUp(self):
        root = os.path.dirname(os.path.realpath(__file__))
        self.out_dir = Path(root, 'out_file')
        self.out_path = Path(self.out_dir, 'ais_maintenance')
        self.out_path.mkdir(parents=True, exist_ok=True)
        self.file_type = 'parquet'
        self.cleanup = True

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        self.assertTrue(self.out_path.exists())
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['FILE_TYPE'] = self.file_type
        os.environ['DB_CONFIG_SOURCE'] = 'environment'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        main()
        file_1_name = f'NEON.DOM.SITE.DP1.00026.001.ais_maintenanceGroundwater_pub.{self.file_type}'
        file_2_name = f'NEON.DOM.SITE.DP1.00026.001.ais_maintenance_pub.{self.file_type}'
        file_3_name = f'NEON.DOM.SITE.DP1.00026.001.ais_maintenanceSensorReplacement_pub.{self.file_type}'
        file_1 = Path(self.out_path, file_1_name)
        file_2 = Path(self.out_path, file_2_name)
        file_3 = Path(self.out_path, file_3_name)
        self.assertTrue(file_1.exists())
        self.assertTrue(file_2.exists())
        self.assertTrue(file_3.exists())
        self.view_file(file_1)
        self.view_file(file_2)
        self.view_file(file_3)
        if self.cleanup:
            self.remove_directories()

    def test_load_files(self):
        data_reader = DataReader(get_tables=get_tables,
                                 get_fields=get_fields,
                                 get_results=get_results,
                                 get_result_values=get_result_values)
        load_files(self.out_path, data_reader, self.file_type)
        file_name = f'NEON.DOM.SITE.DP1.00026.001.ais_maintenanceGroundwater_pub.{self.file_type}'
        file_path = Path(self.out_path, file_name)
        self.assertTrue(file_path.exists())
        self.view_file(file_path)
        if self.cleanup:
            self.remove_directories()

    def view_file(self, file_path):
        if self.file_type == 'csv':
            view_csv_file(file_path)
        if self.file_type == 'parquet':
            view_parquet_file(file_path)

    def remove_directories(self):
        for path in self.out_path.glob('*'):
            if path.is_file():
                path.unlink(missing_ok=True)
        self.out_path.rmdir()
        self.out_dir.rmdir()


def view_csv_file(file_path: Path):
    with closing(open(file_path, 'r', encoding='UTF8')) as file:
        print()
        print(f'file: {file_path}')
        csv_reader = csv.reader(file)
        for line in csv_reader:
            print(line)


def view_parquet_file(file_path: Path):
    print()
    table = pq.read_table(file_path)
    print(table.to_pandas())


if __name__ == '__main__':
    unittest.main()
