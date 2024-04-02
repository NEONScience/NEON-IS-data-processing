#!/usr/bin/env python3
import csv
import os
import unittest
from contextlib import closing
from pathlib import Path

from pyarrow import parquet as pq

from data_access.tests.database_test import DatabaseBackedTest
from os_table_loader.data.data_loader import DataLoader
from os_table_loader.file_writer import write_files
from os_table_loader.main import main
from os_table_loader.tests.data.field_loader import get_fields
from os_table_loader.tests.data.result_values_loader import get_result_values
from os_table_loader.tests.data.result_loader import get_results, get_site_results
from os_table_loader.tests.data.table_loader import get_tables


class LoaderTest(DatabaseBackedTest):

    def setUp(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        self.out_dir = Path(script_path, 'out_file')
        self.out_path = Path(self.out_dir, 'ais_maintenance')
        self.out_path.mkdir(parents=True, exist_ok=True)
        self.partial_table_name = 'maintenance'
        self.file_type = 'csv'
        self.cleanup = True

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        self.assertTrue(self.out_path.exists())
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['FILE_TYPE'] = self.file_type
        os.environ['PARTIAL_TABLE_NAME'] = self.partial_table_name
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

    def test_write_files(self):
        data_loader = DataLoader(get_tables=get_tables,
                                 get_fields=get_fields,
                                 get_results=get_results,
                                 get_site_results=get_site_results,
                                 get_result_values=get_result_values)
        write_files(self.out_path, data_loader, self.file_type, self.partial_table_name)
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
        csv_reader = csv.reader(file)
        for line in csv_reader:
            print(line)


def view_parquet_file(file_path: Path):
    print()
    table = pq.read_table(file_path)
    print(table.to_pandas())


if __name__ == '__main__':
    unittest.main()
