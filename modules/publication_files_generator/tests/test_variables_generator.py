#!/usr/bin/env python3
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from publication_files_generator.filename_formatter import format_filename
from publication_files_generator.timestamp import get_timestamp
from publication_files_generator.variables_generator import generate_variables_file


class TestVariablesFileGenerator(TestCase):

    def test_generate_variables_file(self):
        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.out_path = Path('/out')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        real_path = Path('variables_generator_test_files', 'publication_workbook_water_quality.txt')
        target_path = Path(self.in_path, 'publication_workbook_water_quality.txt')
        self.fs.add_real_file(real_path, target_path=target_path)
        publication_workbook = target_path.read_text()
        domain = 'D10'
        site = 'CPER'
        year = '2020'
        month = '01'
        data_product_id = 'DP1.20288.001'
        timestamp = get_timestamp()
        expected_filename = format_filename(
            domain=domain,
            site=site,
            data_product_id=data_product_id,
            file_type='variables',
            timestamp=timestamp,
            extension='csv')
        filename = generate_variables_file(out_path=self.out_path,
                                           domain=domain,
                                           site=site,
                                           year=year,
                                           month=month,
                                           data_product_id=data_product_id,
                                           timestamp=timestamp,
                                           publication_workbook=publication_workbook)
        assert filename == expected_filename
        path = Path(self.out_path, 'CPER', '2020', '01', expected_filename)
        print(f'\nresult:\n{path.read_text()}\n')
