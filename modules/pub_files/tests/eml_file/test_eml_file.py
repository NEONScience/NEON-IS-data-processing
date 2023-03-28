import os
from datetime import datetime
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from pub_files.database_queries.named_locations import NamedLocation
from pub_files.file_writers.eml_file import EmlFile, EmlDatabase
from pub_files.geometry import Geometry
from pub_files.input_files.file_metadata import PathElements, FileMetadata, DataFiles, DataFile
from pub_files.tests.file_processor_data.file_processor_database import get_data_product
from pub_files.timestamp import get_timestamp


def get_named_location(_named_location_name):
    return NamedLocation(location_id=138773,
                         name='CFGLOC101775',
                         description='Central Plains Soil Temp Profile SP2, Z6 Depth',
                         properties=[])


def get_geometry(_location_name: str) -> Geometry:
    return Geometry(geometry='POINT Z (-104.745591 40.815536 1653.9151)', srid=4979)


class EmlTest(TestCase):

    def setUp(self) -> None:
        self.setUpPyfakefs()
        self.timestamp = get_timestamp()
        self.test_files_path = Path(os.path.dirname(__file__))
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)
        self.domain = 'D10'
        self.site = 'CPER'
        self.year = '2020'
        self.month = '01'
        self.data_product_id = 'DP1.00041.01'
        self.add_boilerplate_file()

    def add_boilerplate_file(self) -> None:
        real_path = Path(self.test_files_path, 'boilerplate.xml')
        self.boilerplate_path = Path('/boilerplate.xml')
        self.fs.add_real_file(real_path, target_path=self.boilerplate_path)

    def test_write_file(self):
        boilerplate = self.boilerplate_path.read_text('UTF-8')
        elements = PathElements(domain=self.domain,
                                site=self.site,
                                year=self.year,
                                month=self.month,
                                data_product_id=self.data_product_id)
        now = datetime.now()
        data_files = DataFiles(files=[DataFile(filename='', description='')], min_time=now, max_time=now)
        data_product = get_data_product(self.fs, _data_product_id='')
        metadata = FileMetadata(path_elements=elements, data_files=data_files, data_product=data_product)
        database = EmlDatabase(get_geometry=get_geometry, get_named_location=get_named_location)
        filename = EmlFile(self.out_path, metadata, boilerplate, self.timestamp, database=database).write()
        assert Path(self.out_path, self.site, self.year, self.month, filename).exists()
