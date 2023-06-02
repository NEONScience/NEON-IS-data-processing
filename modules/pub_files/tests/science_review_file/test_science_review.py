#!/usr/bin/env python3
import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from typing import List

from pyfakefs.fake_filesystem import FakeFilesystem
from pyfakefs.fake_filesystem_unittest import TestCase

from pub_files.data_product import DataProduct
from pub_files.database.file_variables import FileVariables
from pub_files.database.science_review_flags import ScienceReviewFlag
from pub_files.input_files.file_metadata import FileMetadata, DataFile, PathElements, DataFiles
from pub_files.main import get_timestamp
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review import write_file
from pub_files.output_files.variables.variables_file_database import VariablesDatabase


class ScienceReviewFileTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.test_files_path = Path(os.path.dirname(__file__))
        self.in_path = Path('/in/CPER/2020/01')
        self.out_path = Path('/out/CPER/2020/01/basic')
        self.fs.create_dir(self.in_path)
        self.fs.create_dir(self.out_path)

    def test_write(self):
        data_files = DataFiles([get_data_file()], datetime.now(), datetime.now())
        path_elements = get_path_elements()
        file_metadata = FileMetadata()
        file_metadata.path_elements = path_elements
        file_metadata.data_files = data_files
        file_metadata.manifest_file = 'manifest.csv'
        file_metadata.data_product = get_data_product(path_elements.data_product_id)
        file_metadata.package_output_path = self.out_path
        timestamp = get_timestamp()
        variables_database = VariablesDatabase(get_sensor_positions=None,
                                               get_is_science_review=self.get_is_science_review,
                                               get_sae_science_review=None)
        # write the file
        file_path = write_file(file_metadata, 'basic', timestamp, variables_database, get_flags, get_term_name)
        # check the output
        expected_filename = get_filename(elements=path_elements,
                                         file_type='science_review_flags',
                                         timestamp=timestamp,
                                         extension='csv')
        assert file_path.name == expected_filename
        print(f'\n\nfile contents:\n\n{file_path.read_text()}\n')

    def get_is_science_review(self) -> List[FileVariables]:
        """Returns test variables."""
        return self.load_file_variables(self.fs)

    def load_file_variables(self, fs: FakeFilesystem) -> List[FileVariables]:
        """Load variables from JSON file."""
        path = Path(self.test_files_path, 'file_variables.json')
        target_path = Path('/file_variables.json')
        fs.add_real_file(path, target_path=target_path)
        file_variables = []
        with open(target_path) as file:
            json_data = json.load(file)
            for entry in json_data:
                term_name: str = entry['term_name']
                rank: str = entry['rank']
                download_package: str = entry['download_package']
                publication_format: str = entry['pub_format']
                file_variables.append(FileVariables(data_type='dataType',
                                                    description='Description',
                                                    download_package=download_package,
                                                    publication_format=publication_format,
                                                    rank=int(rank),
                                                    table_name='is_science_review',
                                                    term_name=term_name,
                                                    units='units'))
        return file_variables


def get_flags(_data_product_id, _site) -> List[ScienceReviewFlag]:
    """Mock function to return the flags."""
    start_date = datetime.now()
    end_date = datetime.now()
    stream_name = 'NEON.D10.CPER.DP1.00041.001.03937.000.040.030'
    user_name = 'username@battelleecology.org'
    user_comment = 'Suspected mis-application of calibration: Issue resolved by reprocessing - flag removed.'
    create_date = datetime.now()
    last_update = datetime.now()
    flag = ScienceReviewFlag(id=100,
                             start_date=start_date,
                             end_date=end_date,
                             stream_name=stream_name,
                             user_name=user_name,
                             user_comment=user_comment,
                             flag=1,
                             create_date=create_date,
                             last_update=last_update)
    return [flag]


def get_term_name(_term_number) -> str:
    """Mock function to return the term name."""
    return 'termName'


def get_data_file() -> DataFile:
    """Create a test DataFile object."""
    name = 'NEON.D10.CPER.DP1.00041.001.002.506.001.ST_1_minute.2020-01-02.basic.csv'
    description = 'File description'
    line_count = 100
    return DataFile(name, description, line_count)


def get_path_elements() -> PathElements:
    """Create a test PathElements object."""
    domain = 'D10'
    site = 'CPER'
    year = '2020'
    month = '01'
    data_product_id = 'DP1.00041.001'
    return PathElements(domain, site, year, month, data_product_id)


def get_data_product(data_product_id) -> DataProduct:
    """Create a test DataProduct object."""
    return DataProduct(abstract='Abstract',
                       basic_description='Basic description of data product.',
                       category='Category',
                       data_product_id=data_product_id,
                       description='Data product description.',
                       design_description='Design description.',
                       expanded_description='Expanded description',
                       name='Data product name.',
                       remarks='Data product remarks',
                       sensor='Sensor.',
                       short_name='Short name.',
                       study_description='Study description.',
                       supplier='TIS',
                       type_name='The type name.')


if __name__ == '__main__':
    unittest.main()
