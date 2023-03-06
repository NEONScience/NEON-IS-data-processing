"""
Module to generate a readme file and write it to the filesystem.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, NamedTuple, Dict, Tuple

import structlog
import pandas
from jinja2 import Template

import common.date_formatter as date_formatter
from readme_generator.change_log import get_change_log, ChangeLog
from readme_generator.database_queries.log_entries import LogEntry
from readme_generator.database_queries.data_product import DataProduct
from readme_generator.path_parser import parse_path, parse_filename, FilenameParts
from readme_generator.database_queries.location_geometry import get_point_coordinates

log = structlog.get_logger()


class DataFile(NamedTuple):
    name: str
    description: str


class Paths(NamedTuple):
    in_path: Path
    out_path: Path
    path_parse_index: int


class DataFunctions(NamedTuple):
    get_log_entries: Callable[[str], List[LogEntry]]
    get_data_product: Callable[[str], DataProduct]
    get_geometry: Callable[[str], str]
    get_descriptions: Callable[[], Dict[str, str]]
    get_keywords: Callable[[str], List[str]]


def get_readme_filename(domain: str, site: str, idq: str, d: datetime) -> str:
    date = d.strftime('%Y%m%dT%H%M%SZ')
    return f'NEON.{domain}.{site}.{idq}.{date}.readme.txt'


def get_time_span(data_file_path: Path) -> Tuple[str, str]:
    data_frame = pandas.read_csv(data_file_path)
    min_start = data_frame.loc[0][0]  # First row, first element should be earliest start date.
    max_end = data_frame.iloc[-1].tolist()[1]  # Last row, second element should be latest end date.
    return min_start, max_end


def link_file(path: Path, link_path: Path) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        log.debug(f'Linking file: {path} to: {link_path}')
        link_path.symlink_to(path)


def generate_readme(paths: Paths, functions: DataFunctions, readme_template: str) -> None:
    file_descriptions = functions.get_descriptions()

    site = None
    domain = None
    data_product_id = None
    year = ''
    month = ''
    data_files = []
    oldest_data_date = None
    newest_data_date = None
    is_first_file = True
    for path in paths.in_path.rglob('*'):
        if path.is_file():
            site, year, month, day, filename = parse_path(path, paths.path_parse_index)
            if filename != 'manifest.csv':
                parts: FilenameParts = parse_filename(filename)
                domain = parts.domain
                level = parts.level
                data_product_number = parts.data_product_number
                revision = parts.revision
                data_product_id = f'NEON.DOM.SITE.{level}.{data_product_number}.{revision}'
                description = file_descriptions.get(data_product_id)
                if not description:
                    description = None
                data_files.append(DataFile(name=filename, description=description))
                min_time, max_time = get_time_span(path)
                min_date = date_formatter.to_datetime(min_time)
                max_date = date_formatter.to_datetime(max_time)
                if is_first_file:
                    oldest_data_date = min_date
                    newest_data_date = max_date
                    is_first_file = False
                else:
                    if min_date < oldest_data_date:
                        oldest_data_date = min_date
                    if max_date > newest_data_date:
                        newest_data_date = max_date
            link_path = Path(paths.out_path, site, year, month, day, path.name)
            link_file(path, link_path)

    now: datetime = datetime.now(timezone.utc)
    data_product: DataProduct = functions.get_data_product(data_product_id)
    keywords: List[str] = functions.get_keywords(data_product_id)
    log_entries: List[LogEntry] = functions.get_log_entries(data_product_id)
    change_log_entries: List[ChangeLog] = get_change_log(data_product_id, log_entries)
    geometry: str = functions.get_geometry(site)
    coordinates: str = get_point_coordinates(geometry)
    data_file_count: int = len(data_files)
    readme_data = dict(now=now,
                       site=site,
                       domain=domain,
                       data_product=data_product,
                       keywords=keywords,
                       data_start_date=oldest_data_date,
                       data_end_date=newest_data_date,
                       coordinates=coordinates,
                       data_file_count=data_file_count,
                       data_files=data_files,
                       change_logs=change_log_entries)
    template = Template(readme_template, trim_blocks=True, lstrip_blocks=True)
    readme_content = template.render(readme_data)
    readme_filename = get_readme_filename(domain, site, data_product_id, now)
    readme_path = Path(paths.out_path, site, year, month, readme_filename)
    readme_path.write_text(readme_content)

    log.debug(f'Readme file: {readme_filename}')
    log.debug(f'\n\n{readme_content}\n')
