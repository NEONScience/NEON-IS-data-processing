#!/usr/bin/env python3
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem_unittest import TestCase

from pub_files.database.file_variables import FileVariables
from pub_files.database.publication_workbook import PublicationWorkbook
from pub_files.input_files.file_metadata import PathElements
from pub_files.main import get_timestamp
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.variables.variables_file import write_file
from pub_files.output_files.variables.variables_file_database import VariablesDatabase
from pub_files.tests.publication_workbook.publication_workbook import get_workbook


class VariablesFileTest(TestCase):

    def test_write_file(self):
        self.workbook: PublicationWorkbook = get_workbook('')
        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.fs.create_dir(self.in_path)
        domain = 'D10'
        site = 'CPER'
        year = '2020'
        month = '01'
        self.out_path = Path('/out', site, year, month)
        self.fs.create_dir(self.out_path)
        data_product_id = 'DP1.20288.001'
        timestamp = get_timestamp()
        elements = PathElements(domain=domain, site=site, year=year, month=month, data_product_id=data_product_id)
        expected_filename = get_filename(elements, file_type='variables', timestamp=timestamp, extension='csv')
        path = write_file(out_path=self.out_path, elements=elements, timestamp=timestamp, workbook=self.workbook,
                          database=get_mock_database())
        assert path.name == expected_filename
        path = Path(self.out_path, expected_filename)
        print(f'\nresult:\n{path.read_text()}\n')


def get_mock_database() -> VariablesDatabase:
    description = 'description'
    rank = 1
    download_package = 'basic'
    publication_format = '*.##(round)'
    data_type = 'data_type'
    units = 'units'

    def get_sensor_positions() -> List[FileVariables]:
        return [FileVariables(table_name='sensor_positions',
                              term_name='sensor_positions',
                              description=description,
                              rank=rank,
                              download_package=download_package,
                              publication_format=publication_format,
                              data_type=data_type,
                              units=units)]

    def get_is_science_review() -> List[FileVariables]:
        return [FileVariables(table_name='is_science_review',
                              term_name='is_science_review',
                              description=description,
                              rank=rank,
                              download_package=download_package,
                              publication_format=publication_format,
                              data_type=data_type,
                              units=units)]

    def get_sae_science_review() -> List[FileVariables]:
        return [FileVariables(table_name='sae_science_review',
                              term_name='sae_science_review',
                              description=description,
                              rank=rank,
                              download_package=download_package,
                              publication_format=publication_format,
                              data_type=data_type,
                              units=units)]

    return VariablesDatabase(get_sensor_positions=get_sensor_positions,
                             get_is_science_review=get_is_science_review,
                             get_sae_science_review=get_sae_science_review)
