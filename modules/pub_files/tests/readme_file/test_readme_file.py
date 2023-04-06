#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

import pub_files.input_files.file_processor as file_processor
import pub_files.tests.file_processor_data.file_processor_database as file_processor_data
import pub_files.tests.readme_file.readme_database as file_database
from pub_files.output_files.filename_format import format_timestamp
from pub_files.output_files.readme.readme_file import write_file
from pub_files.publication_workbook import PublicationWorkbook
from pub_files.tests.publication_workbook.publication_workbook import get_workbook
from pub_files.main import get_timestamp


class ReadmeFileTest(TestCase):

    def setUp(self):
        self.workbook = get_workbook()
        self.setUpPyfakefs()
        self.test_files_path = Path(os.path.dirname(__file__))
        self.in_path = Path('/in/CPER/2020/01')
        self.out_path = Path('/out')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        self.add_template_file()
        self.add_data_files()

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
        start_time, end_time = file_processor.get_time_span(path)
        assert start_time == '2020-01-02T00:00:00Z'
        assert end_time == '2020-01-03T00:00:00Z'

    def test_write_file(self):
        readme_template = self.template_path.read_text()
        file_processor_database = file_processor_data.get_database(self.fs)
        publication_workbook = PublicationWorkbook(self.workbook)
        file_metadata = file_processor.process(self.in_path, self.out_path, 1, publication_workbook,
                                               file_processor_database)
        timestamp = get_timestamp()
        formatted_timestamp = format_timestamp(timestamp)
        variables_filename = f'NEON.D10.CPER.DP1.0041.{formatted_timestamp}.variables.csv'
        positions_filename = f'NEON.D10.CPER.DP1.0041.{formatted_timestamp}.sensor_positions.csv'
        eml_filename = f'NEON.D10.CPER.DP1.0041.EML.20200102-2020-0103.{formatted_timestamp}.xml'
        write_file(out_path=self.out_path,
                   file_metadata=file_metadata,
                   readme_template=readme_template,
                   timestamp=timestamp,
                   variables_filename=variables_filename,
                   positions_filename=positions_filename,
                   eml_filename=eml_filename,
                   database=file_database.get_database(self.fs))
        readme_files = list(Path(self.out_path, 'CPER', '2020', '01').glob('*.txt'))
        csv_files = list(Path(self.out_path, 'CPER', '2020', '01').glob('*.csv'))
        assert len(readme_files) == 1
        assert len(csv_files) == 5
