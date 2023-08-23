from pathlib import Path
from typing import NamedTuple

import structlog

log = structlog.get_logger()

class PathParts(NamedTuple):
    """Class to hold the needed data of a path."""
    product: str
    site: str
    year: str
    month: str


def parse_path(path: Path, relative_path_index: int) -> PathParts:
    """
    Parse the needed data from an input file path.

    :param path: An input file path.
    :param relative_path_index: The element index to begin parsing the path elements.
    """
    parts = path.parts
    product = parts[relative_path_index]
    site = parts[relative_path_index+1]
    year = parts[relative_path_index+2]
    month = parts[relative_path_index+3]
    return PathParts(product=product, site=site, year=year, month=month)
