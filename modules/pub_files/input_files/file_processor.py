from pathlib import Path
from typing import Tuple

import pandas
import structlog

import common.date_formatter as date_formatter
from pub_files.input_files.file_metadata import DataFile, FileMetadata, PathElements, DataFiles
from pub_files.input_files.file_processor_database import FileProcessorDatabase
from pub_files.input_files.path_parser import parse_path, PathParts
from pub_files.input_files.filename_parser import parse_filename, FilenameData
from pub_files.publication_workbook import PublicationWorkbook


log = structlog.get_logger()


def process(*, in_path: Path, out_path: Path, in_path_parse_index: int, package_type: str,
            publication_workbook: PublicationWorkbook, database: FileProcessorDatabase) -> FileMetadata:
    data_files = []
    min_data_time = None
    max_data_time = None
    domain = None
    site = None
    year = None
    month = None
    data_product_id = None
    is_first_file = True
    for path in in_path.rglob('*.csv'):
        if path.is_file() and path.name != 'manifest.csv' and package_type in path.name:
            path_parts: PathParts = parse_path(path, in_path_parse_index)
            site = path_parts.site
            year = path_parts.year
            month = path_parts.month
            line_count = sum(1 for line in open(path))
            filename_data: FilenameData = parse_filename(path.name)
            domain = filename_data.domain
            level = filename_data.level
            data_product_number = filename_data.data_product_number
            revision = filename_data.revision
            data_product_id = get_data_product_id(level, data_product_number, revision)
            description = publication_workbook.get_file_description(filename_data)
            if not description:
                description = None
            data_file = DataFile(filename=path.name, description=description, line_count=line_count)
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
            # link the file into the output directory.
            link_path = Path(out_path, site, year, month, package_type, path.name)
            link_file(path, link_path)
    path_elements = PathElements(domain=domain, site=site, year=year, month=month, data_product_id=data_product_id)
    data_files = DataFiles(files=data_files, min_time=min_data_time, max_time=max_data_time)
    data_product = database.get_data_product(data_product_id)
    return FileMetadata(path_elements=path_elements, data_files=data_files, data_product=data_product)


def get_time_span(file_path: Path) -> Tuple[str, str]:
    data_frame = pandas.read_csv(file_path)
    min_start = data_frame.loc[0][0]  # First row, first element is the earliest start date.
    max_end = data_frame.iloc[-1].tolist()[1]  # Last row, second element is the latest end date.
    return min_start, max_end


def get_data_product_id(level: str, data_product_number: str, revision: str):
    return f'NEON.DOM.SITE.{level}.{data_product_number}.{revision}'


def link_file(path: Path, link_path: Path) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        log.debug(f'Linking file: {path} to: {link_path}')
        link_path.symlink_to(path)
