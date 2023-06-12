#!/usr/bin/env python3
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem_unittest import TestCase

from pub_files.database.file_variables import FileVariables
from pub_files.database.publication_workbook import PublicationWorkbook
from pub_files.database.term_variables import TermVariables
from pub_files.input_files.file_metadata import PathElements, DataFile, DataFiles, FileMetadata
from pub_files.main import get_timestamp
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile, Term
from pub_files.output_files.variables.variables_file import write_file, format_data_product_name
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
        self.elements = PathElements(domain='D10', site=site, year=year, month=month, data_product_id='DP1.20288.001')
        self.file_metadata = self.get_file_metadata()

    @staticmethod
    def test_name_format():
        dp_name = 'NEON.DOM.SITE.DP1.00041.01.001.002.030'
        formatted = format_data_product_name(dp_name, '00461')
        assert formatted == 'NEON.DOM.SITE.DP1.00041.01.00461.HOR.VER.030'

    def test_write_file(self):
        expected_filename = get_filename(self.elements, file_type='variables', timestamp=self.timestamp,
                                         extension='csv')
        science_review_file = self.get_science_review_file()
        path = write_file(out_path=self.out_path,
                          file_metadata=self.file_metadata,
                          timestamp=self.timestamp,
                          workbook=self.workbook,
                          science_review_file=science_review_file,
                          get_sensor_position_variables=get_sensor_position_variables,
                          get_term_variables=get_term_variables)
        assert path.name == expected_filename
        path = Path(self.out_path, expected_filename)
        with open(path) as file:
            row_count = sum(1 for row in file)
        assert row_count == 71
        print(f'\nresult:\n{path.read_text()}\n')

    def get_science_review_file(self) -> ScienceReviewFile:
        """Returns mock object."""
        filename = get_filename(self.elements, file_type='sensor_review_flags', timestamp=self.timestamp,
                                extension='csv')
        path = Path(self.out_path, filename)
        term = Term(name='term_name', number='000461')
        return ScienceReviewFile(path, 'NEON.DOM.SITE.DP1.20288.001.HOR.VER.030', [term])

    def get_file_metadata(self) -> FileMetadata:
        """Returns mock object."""
        data_file = DataFile(data_product_name='NEON.D10.CPER.DP1.00041.001.002.506.030',
                             description='File description.',
                             line_count=20,
                             filename='NEON.D10.CPER.DP1.00041.001.002.506.030.ST_30_minute.2020-01-02.basic.csv')
        data_files = DataFiles(files=[data_file], min_time=get_timestamp(), max_time=get_timestamp())
        file_metadata = FileMetadata()
        file_metadata.path_elements = self.elements
        file_metadata.data_files = data_files
        file_metadata.data_product = None
        file_metadata.manifest_file = None
        file_metadata.package_output_path = Path(self.out_path)
        return file_metadata


def get_sensor_position_variables() -> List[FileVariables]:
    """Mock function."""
    description = 'description'
    rank = 1
    download_package = 'basic'
    publication_format = '*.##(round)'
    data_type = 'data_type'
    units = 'units'
    return [FileVariables(table_name='sensor_positions',
                          term_name='termName',
                          description=description,
                          rank=rank,
                          download_package=download_package,
                          publication_format=publication_format,
                          data_type=data_type,
                          units=units)]


def get_term_variables(_data_product_id, _term_name) -> TermVariables:
    """Mock function."""
    return TermVariables(description='description',
                         download_package='basic',
                         publication_format='format',
                         data_type='data_type',
                         units='units')
