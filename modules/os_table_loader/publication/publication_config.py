from pathlib import Path
from typing import NamedTuple

from os_table_loader.data.data_loader import DataLoader


class PathConfig(NamedTuple):
    input_path: Path
    workbook_path: Path
    out_path: Path
    input_path_parse_index: int
    data_product_path_index: int
    year_path_index: int
    month_path_index: int
    site_path_index: int
    package_type_path_index: int


class PublicationConfig(NamedTuple):
    path_config: PathConfig
    data_loader: DataLoader
    file_type: str
    partial_table_name: str
