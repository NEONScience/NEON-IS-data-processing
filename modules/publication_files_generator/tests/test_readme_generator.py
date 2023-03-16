#!/usr/bin/env python3
import logging
import os
from functools import partial
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from publication_files_generator.filename_formatter import format_timestamp
from publication_files_generator.readme_generator import generate_readme_file
from publication_files_generator.file_processor import get_time_span, process_input_files
import publication_files_generator.tests.file_data as file_data
from publication_files_generator.timestamp import get_timestamp

log = logging.getLogger()


class ReadmeFileGeneratorTest(TestCase):

    def setUp(self):
        self.dir = os.path.dirname(__file__)
        self.setUpPyfakefs()
        self.test_files_path = Path(self.dir, 'readme_generator_test_files')
        self.in_path = Path('/in/CPER/2020/01/02')
        self.out_path = Path('/out')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        self.add_template_file()
        self.add_data_files()

    def add_template_file(self) -> None:
        real_path = Path(self.dir, 'readme_generator_test_files/template.j2')
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
        start_time, end_time = get_time_span(path)
        assert start_time == '2020-01-02T00:00:00Z'
        assert end_time == '2020-01-03T00:00:00Z'

    def test_generate_readme(self):
        readme_template = self.template_path.read_text()
        get_data_product_partial = partial(file_data.get_data_product, self.fs)
        get_descriptions_partial = partial(file_data.get_descriptions, self.fs)
        get_log_entries_partial = partial(file_data.get_log_entries, self.fs)
        file_metadata = process_input_files(self.in_path, self.out_path, 1, get_descriptions_partial)
        timestamp = get_timestamp()
        formatted_timestamp = format_timestamp(timestamp)
        variables_filename = f'NEON.D10.CPER.DP1.0041.{formatted_timestamp}.variables.csv'
        positions_filename = f'NEON.D10.CPER.DP1.0041.{formatted_timestamp}.sensor_positions.csv'
        generate_readme_file(out_path=self.out_path,
                             file_metadata=file_metadata,
                             readme_template=readme_template,
                             timestamp=timestamp,
                             variables_filename=variables_filename,
                             positions_filename=positions_filename,
                             get_data_product=get_data_product_partial,
                             get_geometry=file_data.get_geometry,
                             get_keywords=file_data.get_keywords,
                             get_log_entries=get_log_entries_partial)
        readme_files = list(Path(self.out_path, 'CPER', '2020', '01').glob('*.txt'))
        csv_files = list(Path(self.out_path, 'CPER', '2020', '01', '02').glob('*.csv'))
        assert len(readme_files) == 1
        assert len(csv_files) == 5
