from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Tuple, List, Dict, Callable

import pandas
import structlog

import common.date_formatter as date_formatter
from publication_files_generator.path_parser import parse_path, parse_filename, FilenameParts

log = structlog.get_logger()


class DataFile(NamedTuple):
    filename: str
    description: str


class InputFileMetadata(NamedTuple):
    domain: str
    site: str
    year: str
    month: str
    data_product_id: str
    data_files: List[DataFile]
    min_time: datetime
    max_time: datetime


def link_file(path: Path, link_path: Path) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        log.debug(f'Linking file: {path} to: {link_path}')
        link_path.symlink_to(path)


def get_time_span(data_file_path: Path) -> Tuple[str, str]:
    data_frame = pandas.read_csv(data_file_path)
    min_start = data_frame.loc[0][0]  # First row, first element is the earliest start date.
    max_end = data_frame.iloc[-1].tolist()[1]  # Last row, second element is the latest end date.
    return min_start, max_end


def get_data_product_id(level: str, data_product_number: str, revision: str):
    return f'NEON.DOM.SITE.{level}.{data_product_number}.{revision}'


def process_input_files(in_path: Path, out_path: Path, in_path_parse_index: int,
                        get_descriptions: Callable[[], Dict[str, str]]) -> InputFileMetadata:
    site = ''
    domain = ''
    year = ''
    month = ''
    data_product_id = ''
    data_files = []
    min_data_time = None
    max_data_time = None
    is_first_file = True
    file_descriptions = get_descriptions()
    for path in in_path.rglob('*'):
        if path.is_file():
            site, year, month, filename = parse_path(path, in_path_parse_index)
            if filename != 'manifest.csv':
                parts: FilenameParts = parse_filename(filename)
                domain = parts.domain
                level = parts.level
                data_product_number = parts.data_product_number
                revision = parts.revision
                data_product_id = get_data_product_id(level, data_product_number, revision)
                description = file_descriptions.get(data_product_id)
                if not description:
                    description = None
                data_files.append(DataFile(filename=filename, description=description))
                min_time, max_time = get_time_span(path)
                min_date = date_formatter.to_datetime(min_time)
                max_date = date_formatter.to_datetime(max_time)
                if is_first_file:
                    min_data_time = min_date
                    max_data_time = max_date
                    is_first_file = False
                else:
                    if min_date < min_data_time:
                        min_data_time = min_date
                    if max_date > max_data_time:
                        max_data_time = max_date
            # Link the file into the output directory.
            link_path = Path(out_path, site, year, month, path.name)
            link_file(path, link_path)
    return InputFileMetadata(domain=domain,
                             site=site,
                             year=year,
                             month=month,
                             data_product_id=data_product_id,
                             data_files=data_files,
                             min_time=min_data_time,
                             max_time=max_data_time)
