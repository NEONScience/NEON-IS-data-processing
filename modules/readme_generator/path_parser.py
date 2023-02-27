from pathlib import Path
from typing import NamedTuple


class FilenameParts(NamedTuple):
    site: str
    domain: str
    level: str
    dp_number: str
    revision: str


class PathParts(NamedTuple):
    site: str
    year: str
    month: str
    day: str
    filename: str


def parse_path(path: Path) -> PathParts:
    parts = path.parts
    site = parts[2]
    year = parts[3]
    month = parts[4]
    day = parts[5]
    filename = path.name
    return PathParts(site=site, year=year, month=month, day=day, filename=filename)


def parse_filename(filename: str) -> FilenameParts:
    parts = filename.split('.')
    domain = parts[1]
    site = parts[2]
    level = parts[3]
    dp_number = parts[4]
    revision = parts[5]
    return FilenameParts(domain=domain, site=site, level=level,  dp_number=dp_number, revision=revision)
