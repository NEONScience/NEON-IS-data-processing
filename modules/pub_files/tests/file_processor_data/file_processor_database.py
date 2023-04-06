import json
import os
from functools import partial
from pathlib import Path

from pyfakefs.fake_filesystem import FakeFilesystem

from pub_files.data_product import DataProduct
from pub_files.input_files.file_processor_database import FileProcessorDatabase


def get_database(fs: FakeFilesystem) -> FileProcessorDatabase:
    get_data_product_partial = partial(get_data_product, fs)
    return FileProcessorDatabase(get_data_product=get_data_product_partial)


def get_data_product(fs: FakeFilesystem, _data_product_id: str) -> DataProduct:
    """Mock function for reading the data product."""
    path = Path(os.path.dirname(__file__), 'data_product.json')
    target_path = Path('/data_product.json')
    fs.add_real_file(path, target_path=target_path)
    with open(target_path) as file:
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
