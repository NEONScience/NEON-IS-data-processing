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


class PathParts(NamedTuple):
    site: str
    year: str
    month: str
    day: str
    filename: str


def parse_path(path: Path, path_parse_index: int) -> PathParts:
    parts = path.parts
    site = parts[path_parse_index+1]
    year = parts[path_parse_index+2]
    month = parts[path_parse_index+3]
    day = parts[path_parse_index+4]
    filename = path.name
    return PathParts(site=site, year=year, month=month, day=day, filename=filename)


def parse_filename(filename: str) -> FilenameParts:
    parts = filename.split('.')
    domain = parts[1]
    site = parts[2]
    level = parts[3]
    data_product_number = parts[4]
    revision = parts[5]
    return FilenameParts(domain=domain,
                         site=site,
                         level=level,
                         data_product_number=data_product_number,
                         revision=revision)
