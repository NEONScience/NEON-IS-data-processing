#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import pub_files.tests.readme_file.readme_database as file_database
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.input_files.file_processor import process_files, get_file_time_span, PublicationPackage
from pub_files.main import get_timestamp
from pub_files.output_files.filename_format import format_timestamp
from pub_files.output_files.readme.readme_file import write_file
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile
from pub_files.tests.input_file_processor_data.file_processor_database import FileProcessorDatabaseMock


class ReadmeFileTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.test_files_path = Path(os.path.dirname(__file__))
        self.in_path = Path('/in/CPER/2020/01')
        self.out_path = Path('/out/CPER/2020/01/basic')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        self.add_template_file()
        self.add_data_files()
        self.add_workbook()

    def add_workbook(self) -> None:
        path = Path(self.test_files_path, '../publication_workbook/soil_temperature_publication_workbook.txt')
        self.fs.add_real_file(path, target_path=path)

    def add_template_file(self) -> None:
        real_path = Path(self.test_files_path, 'template.j2')
        self.template_path = Path('/templates/template.j2')
        self.fs.add_real_file(real_path, target_path=self.template_path)

    def add_data_files(self) -> None:
        self.add_data_file('manifest.csv')
        self.add_data_file('NEON.D10.CPER.DP1.00041.001.002.506.001.ST_1_minute.2020-01-02.basic.csv')
        self.add_data_file('NEON.D10.CPER.DP1.00041.001.002.506.001.ST_1_minute.2020-01-02.expanded.csv')
        self.add_data_file('NEON.D10.CPER.DP1.00041.001.002.506.030.ST_30_minute.2020-01-02.basic.csv')
        self.add_data_file('NEON.D10.CPER.DP1.00041.001.002.506.030.ST_30_minute.2020-01-02.expanded.csv')

    def add_data_file(self, filename):
        real_path = Path(self.test_files_path, filename)
        target_path = Path(self.in_path, filename)
        self.fs.add_real_file(real_path, target_path=target_path)

    def test_get_time_span(self):
        path = Path(self.in_path, 'NEON.D10.CPER.DP1.00041.001.002.506.001.ST_1_minute.2020-01-02.basic.csv')
        start_time, end_time = get_file_time_span(path)
        assert start_time == '2020-01-02T00:00:00Z'
        assert end_time == '2020-01-03T00:00:00Z'

    def test_write_file(self):
        readme_template = self.template_path.read_text()
        mock_database = FileProcessorDatabaseMock(self.fs)
        file_processor_database = mock_database.get_database()
        publication_package: PublicationPackage = process_files(in_path=self.in_path,
                                                                out_path=Path('/out'),
                                                                in_path_parse_index=1,
                                                                database=file_processor_database)
        file_metadata: FileMetadata = publication_package.package_metadata.get('basic')
        timestamp = get_timestamp()
        formatted_timestamp = format_timestamp(timestamp)
        variables_filename = f'NEON.D10.CPER.DP1.00041.{formatted_timestamp}.variables.csv'
        positions_filename = f'NEON.D10.CPER.DP1.00041.{formatted_timestamp}.sensor_positions.csv'
        eml_filename = f'NEON.D10.CPER.DP1.00041.EML.20200102-2020-0103.{formatted_timestamp}.xml'
        science_review_filename = f'NEON.D10.CPER.DP1.00041.{formatted_timestamp}.science_review_flags.csv'
        write_file(out_path=self.out_path,
                   file_metadata=file_metadata,
                   readme_template=readme_template,
                   timestamp=timestamp,
                   variables_filename=variables_filename,
                   positions_filename=positions_filename,
                   eml_filename=eml_filename,
                   science_review_file=ScienceReviewFile(Path(self.out_path, science_review_filename), ['overflowQF']),
                   database=file_database.get_database(self.fs))
        readme_files = list(Path(self.out_path).glob('*.txt'))
        csv_files = list(Path(self.out_path).glob('*.csv'))
        files = list(Path(self.out_path).rglob('*'))
        for file in files:
            print(f'output file: {file}')
        assert len(readme_files) == 1
        assert len(csv_files) == 2
        expected_filename = f'NEON.D10.CPER.DP1.00041.001.readme.{formatted_timestamp}.txt'
        readme_file = readme_files[0]
        assert readme_file.name == expected_filename
        # print(f'\n\ncontent: \n\n{readme_file.read_text()}\n\n')
