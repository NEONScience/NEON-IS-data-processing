#!/usr/bin/env python3
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.variables_file import write_file
from pub_files.publication_workbook import PublicationWorkbook
from pub_files.tests.publication_workbook.publication_workbook import get_workbook
from pub_files.main import get_timestamp


class VariablesFileTest(TestCase):

    def test_write_file(self):
        self.workbook = PublicationWorkbook(get_workbook())
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
        filename = write_file(out_path=self.out_path, elements=elements, timestamp=timestamp, workbook=self.workbook)
        assert filename == expected_filename
        path = Path(self.out_path, expected_filename)
        print(f'\nresult:\n{path.read_text()}\n')
