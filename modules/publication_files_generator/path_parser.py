"""
Module to parse input file paths and filenames.
"""
from pathlib import Path
from typing import NamedTuple


class FilenameParts(NamedTuple):
    site: str
    domain: str
    level: str
    data_product_number: str
    revision: str
    horizontal_index: str
    vertical_index: str
    temporal_index: str


class PathParts(NamedTuple):
    site: str
    year: str
    month: str
    day: str
    filename: str


def parse_path(path: Path, in_path_parse_index: int) -> PathParts:
    parts = path.parts
    site = parts[in_path_parse_index+1]
    year = parts[in_path_parse_index+2]
    month = parts[in_path_parse_index+3]
    day = parts[in_path_parse_index+4]
    filename = path.name
    return PathParts(site=site, year=year, month=month, day=day, filename=filename)


def parse_filename(filename: str) -> FilenameParts:
    parts = filename.split('.')
    domain = parts[1]
    site = parts[2]
    level = parts[3]
    data_product_number = parts[4]
    revision = parts[5]
    horizontal_index = parts[6]
    vertical_index = parts[7]
    temporal_index = parts[8]
    return FilenameParts(domain=domain,
                         site=site,
                         level=level,
                         data_product_number=data_product_number,
                         revision=revision,
                         horizontal_index=horizontal_index,
                         vertical_index=vertical_index,
                         temporal_index=temporal_index)
