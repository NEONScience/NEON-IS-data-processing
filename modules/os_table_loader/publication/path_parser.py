from pathlib import Path
from typing import NamedTuple

from os_table_loader.publication.publication_config import PathConfig


class PathParts(NamedTuple):
    data_product: str
    year: str
    month: str
    site: str
    package_type: str
    metadata_path: Path


def parse_path(path: Path, config: PathConfig) -> PathParts:
    parts = path.parts
    data_product = parts[config.data_product_path_index]
    year = parts[config.year_path_index]
    month = parts[config.month_path_index]
    site = parts[config.site_path_index]
    package_type = parts[config.package_type_path_index]
    metadata_path = Path(*parts[config.input_path_parse_index:]).parent
    return PathParts(data_product=data_product,
                     year=year,
                     month=month,
                     site=site,
                     package_type=package_type,
                     metadata_path=metadata_path)
