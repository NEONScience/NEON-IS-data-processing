#!/usr/bin/env python3
import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from pub_files.input_files.file_metadata import PathElements
from pub_files.file_writers.filename_format import get_filename
from pub_files.timestamp import get_timestamp
from pub_files.file_writers.variables_file import write_file


class VariablesFileTest(TestCase):

    def test_write_file(self):
        self.setUpPyfakefs()
        self.in_path = Path('/in')
        self.out_path = Path('/out')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)
        real_path = Path(os.path.dirname(__file__), 'publication_workbook_water_quality.txt')
        target_path = Path(self.in_path, 'publication_workbook_water_quality.txt')
        self.fs.add_real_file(real_path, target_path=target_path)
        workbook = target_path.read_text()
        domain = 'D10'
        site = 'CPER'
        year = '2020'
        month = '01'
        data_product_id = 'DP1.20288.001'
        timestamp = get_timestamp()
        elements = PathElements(domain=domain, site=site, year=year, month=month, data_product_id=data_product_id)
        expected_filename = get_filename(elements, file_type='variables', timestamp=timestamp, extension='csv')
        filename = write_file(out_path=self.out_path, elements=elements, timestamp=timestamp, workbook=workbook)
        assert filename == expected_filename
        path = Path(self.out_path, 'CPER', '2020', '01', expected_filename)
        print(f'\nresult:\n{path.read_text()}\n')
