#!/usr/bin/env python3
import logging
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase
from readme_generator.generator import generate_readme, Paths, get_time_span
from readme_generator.tests.mock_database import MockDatabase

log = logging.getLogger()


class ReadmeGeneratorTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.test_files_path = Path(os.path.dirname(__file__), 'generator_test_files')
        self.in_path = Path('/in/CPER/2020/01/02')
        self.out_path = Path('/out')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        self.add_template_file()
        self.add_data_files()

    def add_template_file(self) -> None:
        real_path = Path(os.path.dirname(__file__), 'generator_test_files/template.j2')
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
        paths = Paths(in_path=self.in_path, out_path=self.out_path, path_parse_index=1)
        template = self.template_path.read_text()
        database = MockDatabase(self.fs)
        data_functions = database.get_data_functions()
        generate_readme(paths, data_functions, template)
        readme_files = list(Path(self.out_path, 'CPER', '2020', '01').glob('*.txt'))
        csv_files = list(Path(self.out_path, 'CPER', '2020', '01', '02').glob('*.csv'))
        assert len(readme_files) == 1
        assert len(csv_files) == 5
