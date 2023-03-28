from pathlib import Path
from typing import NamedTuple


class PathParts(NamedTuple):
    site: str
    year: str
    month: str
    filename: str


def parse_path(path: Path, in_path_parse_index: int) -> PathParts:
    parts = path.parts
    site = parts[in_path_parse_index+1]
    year = parts[in_path_parse_index+2]
    month = parts[in_path_parse_index+3]
    return PathParts(site=site, year=year, month=month, filename=path.name)
