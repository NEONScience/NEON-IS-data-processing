from datetime import datetime
from pathlib import Path
from typing import NamedTuple, List

from pub_files.data_product import DataProduct
from pub_files.input_files.manifest_file import ManifestFile


class DataFile(NamedTuple):
    filename: str
    description: str
    line_count: int


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


class FileMetadata:
    path_elements: PathElements
    data_files: DataFiles
    data_product: DataProduct
    manifest_file: ManifestFile
    package_output_path: Path
