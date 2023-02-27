#!/usr/bin/env python3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, NamedTuple, Dict, Tuple

import structlog
import pandas

import common.date_formatter as date_formatter
from readme_generator.change_log import get_change_log, ChangeLog
from readme_generator.log_entry import LogEntry
from readme_generator.data_product import DataProduct
from readme_generator.path_parser import parse_path, parse_filename, FilenameParts
from readme_generator.template import render
from readme_generator.location_geometry import get_point_coordinates

log = structlog.get_logger()


class DataFile(NamedTuple):
    name: str
    description: str


class Config(NamedTuple):
    in_path: Path
    out_path: Path
    template_path: Path


def get_readme_filename(domain: str, site: str, idq: str, d: datetime) -> str:
    date = d.strftime('%Y%m%dT%H%M%SZ')
    return f'NEON.{domain}.{site}.{idq}.{date}.readme.txt'


def get_time_span(data_file_path: Path) -> Tuple[str, str]:
    data_frame = pandas.read_csv(data_file_path)
    min_start = data_frame.loc[0][0]  # First row, first element should be earliest start date.
    max_end = data_frame.iloc[-1].tolist()[1]  # Last row, second element should be latest end date.
    return min_start, max_end


def write_file(file_path: Path, content: str) -> None:
    print(f'content:\n\n"\n{content}\n"\n\n')
    with open(file_path, mode='w', encoding='utf-8') as file:
        file.write(content)
        log.debug(f'Wrote {file_path}.')


def link_file(path: Path, link_path: Path) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        log.debug(f'path: {path} link_path: {link_path}')
        link_path.symlink_to(path)


def generate_readme(
        *,
        config: Config,
        get_log_entries: Callable[[str], List[LogEntry]],
        get_data_product: Callable[[str], DataProduct],
        get_geometry: Callable[[str], str],
        get_descriptions: Callable[[], Dict[str, str]],
        get_keywords: Callable[[str], List[str]]) -> None:

    in_path = config.in_path
    out_path = config.out_path
    template_path = config.template_path

    file_descriptions = get_descriptions()

    site = None
    domain = None
    dp_idq = None
    year = ''
    month = ''
    data_files = []
    oldest_data_date = None
    newest_data_date = None

    is_first_loop = True
    for path in in_path.rglob('*'):
        if path.is_file():
            site, year, month, day, filename = parse_path(path)
            if filename != 'manifest.csv':
                parts: FilenameParts = parse_filename(filename)
                domain = parts.domain
                level = parts.level
                dp_number = parts.dp_number
                revision = parts.revision
                dp_idq = f'NEON.DOM.SITE.{level}.{dp_number}.{revision}'
                description = file_descriptions.get(dp_idq)
                if not description:
                    description = None
                data_files.append(DataFile(name=filename, description=description))
                min_time, max_time = get_time_span(path)
                min_date = date_formatter.to_datetime(min_time)
                max_date = date_formatter.to_datetime(max_time)
                if is_first_loop:
                    oldest_data_date = min_date
                    newest_data_date = max_date
                    is_first_loop = False
                else:
                    if min_date < oldest_data_date:
                        oldest_data_date = min_date
                    if max_date > newest_data_date:
                        newest_data_date = max_date
            link_path = Path(out_path, site, year, month, day, path.name)
            link_file(path, link_path)

    now: datetime = datetime.now(timezone.utc)
    data_product: DataProduct = get_data_product(dp_idq)
    keywords: List[str] = get_keywords(dp_idq)
    log_entries: List[LogEntry] = get_log_entries(dp_idq)
    change_log_entries: List[ChangeLog] = get_change_log(dp_idq, log_entries)
    geometry: str = get_geometry(site)
    coordinates: str = get_point_coordinates(geometry)
    data_file_count: int = len(data_files)

    readme_data = {}
    readme_data.update(now=now)
    readme_data.update(site=site)
    readme_data.update(domain=domain)
    readme_data.update(data_product=data_product)
    readme_data.update(keywords=keywords)
    readme_data.update(data_start_date=oldest_data_date)
    readme_data.update(data_end_date=newest_data_date)
    readme_data.update(coordinates=coordinates)
    readme_data.update(data_file_count=data_file_count)
    readme_data.update(data_files=data_files)
    readme_data.update(change_logs=change_log_entries)

    readme_content = render(template_path, readme_data)
    readme_filename = get_readme_filename(domain, site, dp_idq, now)
    readme_path = Path(out_path, site, year, month, readme_filename)
    write_file(readme_path, readme_content)
