import json
import os
from pathlib import Path

from pyfakefs.fake_filesystem import FakeFilesystem

from pub_files.data_product import DataProduct
from pub_files.input_files.file_processor_database import FileProcessorDatabase
from pub_files.tests.publication_workbook.publication_workbook import get_workbook


class FileProcessorDatabaseMock:

    def __init__(self, fs: FakeFilesystem):
        self.fs = fs
        real_path = Path(os.path.dirname(__file__), 'data_product.json')
        self.data_product_path = Path('/data_product.json')
        fs.add_real_file(real_path, target_path=self.data_product_path)
        self.data_product = self.load_product()

    def load_product(self) -> DataProduct:
        with open(self.data_product_path) as file:
            json_data = json.load(file)
            data = json_data[0]
            data_product_id: str = data['dp_idq']
            dp_name: str = data['dp_name']
            dp_description: str = data['dp_desc']
            category: str = data['category']
            supplier: str = data['supplier']
            dp_shortname: str = data['dp_shortname']
            dp_abstract: str = data['dp_abstract']
            design_description: str = data['design_desc']
            study_description: str = data['study_desc']
            sensor: str = data['sensor']
            basic_description: str = data['basic_desc']
            expanded_desc: str = data['expanded_desc']
            remarks: str = data['remarks']
        return DataProduct(data_product_id=data_product_id,
                           name=dp_name,
                           type_name='TIS Data Product Type',
                           description=dp_description,
                           category=category,
                           supplier=supplier,
                           short_name=dp_shortname,
                           abstract=dp_abstract,
                           design_description=design_description,
                           study_description=study_description,
                           sensor=sensor,
                           basic_description=basic_description,
                           expanded_description=expanded_desc,
                           remarks=remarks)

    def get_database(self) -> FileProcessorDatabase:
        return FileProcessorDatabase(get_data_product=self.get_data_product,
                                     get_workbook=get_workbook)

    def get_data_product(self, _data_product_id: str) -> DataProduct:
        """Mock function for reading the data product."""
        return self.data_product


def get_data_product(self, _data_product_id: str) -> DataProduct:
    """Mock function for reading the data product."""
    return self.data_product

