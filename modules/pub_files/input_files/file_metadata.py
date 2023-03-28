from datetime import datetime
from typing import NamedTuple, List

from pub_files.data_product import DataProduct


class DataFile(NamedTuple):
    filename: str
    description: str


class DataFiles(NamedTuple):
    files: List[DataFile]
    min_time: datetime
    max_time: datetime


class PathElements(NamedTuple):
    domain: str
    site: str
    year: str
    month: str
    data_product_id: str


class FileMetadata(NamedTuple):
    path_elements: PathElements
    data_files: DataFiles
    data_product: DataProduct
