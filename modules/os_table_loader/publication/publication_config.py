from pathlib import Path
from typing import NamedTuple

from os_table_loader.data.data_loader import DataLoader


class PublicationConfig(NamedTuple):
    input_path: Path
    workbook_path: Path
    out_path: Path
    input_path_parse_index: int
    data_product_path_index: int
    data_loader: DataLoader
    file_type: str
    partial_table_name: str
