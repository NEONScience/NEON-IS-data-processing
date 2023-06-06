#!/usr/bin/env python3
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem_unittest import TestCase

from pub_files.database.file_variables import FileVariables
from pub_files.database.publication_workbook import PublicationWorkbook
from pub_files.database.term_variables import TermVariables
from pub_files.input_files.file_metadata import PathElements
from pub_files.main import get_timestamp
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile
from pub_files.output_files.variables.variables_file import write_file
from pub_files.output_files.variables.variables_file_database import VariablesDatabase
from pub_files.tests.publication_workbook.publication_workbook import get_workbook


class VariablesFileTest(TestCase):

    def setUp(self) -> None:
        self.workbook: PublicationWorkbook = get_workbook('fake_value')
        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.fs.create_dir(self.in_path)
        site = 'CPER'
        year = '2020'
        month = '01'
        self.out_path = Path('/out', site, year, month)
        self.fs.create_dir(self.out_path)
        self.timestamp = get_timestamp()
        self.elements = PathElements(domain='D10',
                                     site=site,
                                     year=year,
                                     month=month,
                                     data_product_id='DP1.20288.001')

    def test_write_file(self):
        expected_filename = get_filename(self.elements,
                                         file_type='variables',
                                         timestamp=self.timestamp,
                                         extension='csv')
        science_review_file = self.get_science_review_file()
        path = write_file(out_path=self.out_path,
                          elements=self.elements,
                          package_type='basic',
                          timestamp=self.timestamp,
                          workbook=self.workbook,
                          database=get_variables_database(),
                          science_review_file=science_review_file,
                          get_term_variables=get_term_variables)
        assert path.name == expected_filename
        path = Path(self.out_path, expected_filename)
        with open(path) as file:
            row_count = sum(1 for row in file)
        assert row_count == 71
        print(f'\nresult:\n{path.read_text()}\n')

    def get_science_review_file(self) -> ScienceReviewFile:
        filename = get_filename(self.elements,
                                file_type='sensor_review_flags',
                                timestamp=self.timestamp,
                                extension='csv')
        path = Path(self.out_path, filename)
        return ScienceReviewFile(path,
                                 'NEON.DOM.SITE.DP1.20288.001.HOR.VER.030',
                                 ['term_name'])


def get_variables_database() -> VariablesDatabase:
    description = 'description'
    rank = 1
    download_package = 'basic'
    publication_format = '*.##(round)'
    data_type = 'data_type'
    units = 'units'

    def get_sensor_positions() -> List[FileVariables]:
        return [FileVariables(table_name='sensor_positions',
                              term_name='termName',
                              description=description,
                              rank=rank,
                              download_package=download_package,
                              publication_format=publication_format,
                              data_type=data_type,
                              units=units)]

    def get_is_science_review() -> List[FileVariables]:
        return [FileVariables(table_name='is_science_review',
                              term_name='termName',
                              description=description,
                              rank=rank,
                              download_package=download_package,
                              publication_format=publication_format,
                              data_type=data_type,
                              units=units)]

    def get_sae_science_review() -> List[FileVariables]:
        return [FileVariables(table_name='sae_science_review',
                              term_name='termName',
                              description=description,
                              rank=rank,
                              download_package=download_package,
                              publication_format=publication_format,
                              data_type=data_type,
                              units=units)]

    return VariablesDatabase(get_sensor_positions=get_sensor_positions,
                             get_is_science_review=get_is_science_review,
                             get_sae_science_review=get_sae_science_review)


def get_term_variables(_data_product_id, _term_name) -> TermVariables:
    """Mock function."""
    return TermVariables(description='description',
                         download_package='basic',
                         publication_format='format',
                         data_type='data_type',
                         units='units')
