from pathlib import Path
from typing import Tuple, NamedTuple, Callable, Dict

import pandas
import structlog

import common.date_formatter as date_formatter
from publication_files.file_metadata import DataFile, FileMetadata, PathElements, DataFiles
from publication_files.path_parser import parse_path, parse_filename, FilenameParts

log = structlog.get_logger()


class FileProcessorDatabase(NamedTuple):
    get_descriptions: Callable[[], Dict[str, str]]


def process(in_path: Path, out_path: Path, in_path_parse_index: int, database: FileProcessorDatabase) -> FileMetadata:
    file_descriptions = database.get_descriptions()
    site = ''
    domain = ''
    year = ''
    month = ''
    data_product_id = ''
    data_files = []
    min_data_time = None
    max_data_time = None
    is_first_file = True
    for path in in_path.rglob('*'):
        if path.is_file():
            (site, year, month, filename) = parse_path(path, in_path_parse_index)
            if filename != 'manifest.csv':
                parts: FilenameParts = parse_filename(filename)
                domain = parts.domain
                level = parts.level
                data_product_number = parts.data_product_number
                revision = parts.revision
                data_product_id = _get_data_product_id(level, data_product_number, revision)
                description = file_descriptions.get(data_product_id)
                if not description:
                    description = None
                data_file = DataFile(filename=filename, description=description)
                data_files.append(data_file)
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
            _link_file(path, link_path)
    path_elements = PathElements(domain=domain, site=site, year=year, month=month, data_product_id=data_product_id)
    data_files = DataFiles(files=data_files, min_time=min_data_time, max_time=max_data_time)
    return FileMetadata(path_elements=path_elements, data_files=data_files)


def get_time_span(file_path: Path) -> Tuple[str, str]:
    data_frame = pandas.read_csv(file_path)
    min_start = data_frame.loc[0][0]  # First row, first element is the earliest start date.
    max_end = data_frame.iloc[-1].tolist()[1]  # Last row, second element is the latest end date.
    return min_start, max_end


def _get_data_product_id(level: str, data_product_number: str, revision: str):
    return f'NEON.DOM.SITE.{level}.{data_product_number}.{revision}'


def _link_file(path: Path, link_path: Path) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        log.debug(f'Linking file: {path} to: {link_path}')
        link_path.symlink_to(path)
