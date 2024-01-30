#!/usr/bin/env python3
import csv
import os
import unittest
from contextlib import closing
from pathlib import Path

from pyarrow import parquet as pq

from data_access.tests.database_test import DatabaseBackedTest
from os_table_loader.data.data_loader import DataLoader
from os_table_loader.publication.publication_config import PublicationConfig
from os_table_loader.publication.publication_file_writer import write_publication_files
from os_table_loader.publication_main import main
from os_table_loader.tests.data.field_loader import get_fields
from os_table_loader.tests.data.result_values_loader import get_result_values
from os_table_loader.tests.data.result_loader import get_results, get_site_results
from os_table_loader.tests.data.table_loader import get_tables
from pub_files.input_files.manifest_file import ManifestFile


class PublicationWriterTest(DatabaseBackedTest):

    def setUp(self):
        self.view_files = False  # Set to True to view generated files.
        self.setUpPyfakefs()
        self.add_input_files()
        self.add_workbook()
        self.out_path = Path('/', 'output')
        self.out_path.mkdir(parents=True, exist_ok=True)
        self.partial_table_name = 'maintenance'
        self.file_type = 'csv'

    def add_input_files(self):
        self.in_path = Path('/', 'input')
        metadata_path = Path('DP1.20100.001', 'ARIK', '2020', '01')
        file1 = 'NEON.D10.ARIK.DP1.20100.001.003.000.030.RH_30min.2020-01.expanded.20230719T223823Z.csv'
        file2 = 'NEON.D10.ARIK.DP1.20100.001.003.000.030.RH_30min.2020-01.basic.20230719T223823Z.csv'
        self.fs.create_file(Path(self.in_path, metadata_path, file1))
        self.fs.create_file(Path(self.in_path, metadata_path, file2))
        self.fs.create_file(Path(self.in_path, metadata_path, ManifestFile.get_filename()))

    def add_workbook(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        self.workbook_path = Path('/', 'workbook')
        actual_workbook_path = Path(script_path, 'publication_workbook_elevation_of_groundwater.txt')
        workbook_filename =f'publication_workbook_NEON.DOM.SITE.DP1.20100.001.txt'
        self.fs.add_real_file(actual_workbook_path, target_path=Path(self.workbook_path, workbook_filename))

    @unittest.skip('Integration test skipped due to long process time.')
    def test_main(self):
        self.assertTrue(self.out_path.exists())
        os.environ['IN_PATH'] = str(self.in_path)
        os.environ['WORKBOOK_PATH'] = str(self.workbook_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['PARTIAL_TABLE_NAME'] = self.partial_table_name
        os.environ['FILE_TYPE'] = self.file_type
        os.environ['DB_CONFIG_SOURCE'] = 'environment'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['INPUT_PATH_PARSE_INDEX'] = '2'
        os.environ['DATA_PRODUCT_PATH_INDEX'] = '2'
        main()
        for path in self.out_path.rglob('*'):
            if path.is_file():
                if self.view_files:
                    print(f'\n\npath: {path}')
                    view_csv_file(path)

    def test_write_publication_files(self):
        data_loader = DataLoader(get_tables=get_tables,
                                 get_fields=get_fields,
                                 get_results=get_results,
                                 get_site_results=get_site_results,
                                 get_result_values=get_result_values)
        config = PublicationConfig(input_path=self.in_path,
                                   workbook_path=self.workbook_path,
                                   out_path=self.out_path,
                                   data_loader=data_loader,
                                   file_type=self.file_type,
                                   partial_table_name=self.partial_table_name,
                                   input_path_parse_index=2,
                                   data_product_path_index=2)
        write_publication_files(config)
        i = 0
        for path in self.out_path.rglob('*'):
            if path.is_file():
                i += 1
                if self.view_files:
                    print(f'\npath: {path}')
                    view_csv_file(path)
        assert i == 4

    def view_file(self, file_path):
        if self.file_type == 'csv':
            view_csv_file(file_path)
        if self.file_type == 'parquet':
            view_parquet_file(file_path)


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
