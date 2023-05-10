from datetime import datetime
from pathlib import Path
from typing import NamedTuple, List

from pub_files.data_product import DataProduct
from pub_files.input_files.manifest_file import ManifestFile


class DataFile(NamedTuple):
    """Data file descriptions needed for readme file."""
    filename: str
    description: str
    line_count: int


class DataFiles(NamedTuple):
    """Data file metadata for readme file."""
    files: List[DataFile]
    min_time: datetime
    max_time: datetime


class PathElements(NamedTuple):
    """Path components needed for writing metadata files."""
    domain: str
    site: str
    year: str
    month: str
    data_product_id: str


class FileMetadata:
    """Consolidates above classes in a single class."""
    path_elements: PathElements
    data_files: DataFiles
    data_product: DataProduct
    manifest_file: ManifestFile
    package_output_path: Path
