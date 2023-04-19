from pathlib import Path
from typing import Tuple, List

import pandas
import structlog

import common.date_formatter as date_formatter
from pub_files.input_files.file_metadata import DataFile, FileMetadata, PathElements, DataFiles
from pub_files.input_files.file_processor_database import FileProcessorDatabase
from pub_files.input_files.manifest_file import ManifestFile
from pub_files.input_files.path_parser import parse_path
from pub_files.input_files.filename_parser import parse_filename, FilenameParts
from pub_files.publication_workbook import PublicationWorkbook

log = structlog.get_logger()


def process(*, in_path: Path, out_path: Path, in_path_parse_index: int, package_type: str,
            workbook: PublicationWorkbook, database: FileProcessorDatabase) -> FileMetadata:
    file_metadata = FileMetadata()
    (data_paths, manifest_path) = sort_files(in_path, package_type)
    data_files = []
    min_data_time = None
    max_data_time = None
    package_output_path = None
    is_first_file = True
    for path in data_paths:
        line_count = sum(1 for line in open(path))
        (site, year, month) = parse_path(path, in_path_parse_index)
        name_parts: FilenameParts = parse_filename(path.name)
        data_product_id = f'NEON.DOM.SITE.{name_parts.level}.{name_parts.data_product_number}.{name_parts.revision}'
        description = workbook.get_file_description(name_parts)
        data_files.append(DataFile(filename=path.name, description=description, line_count=line_count))
        min_time, max_time = get_time_span(path)
        min_date = date_formatter.to_datetime(min_time)
        max_date = date_formatter.to_datetime(max_time)
        if is_first_file:
            min_data_time = min_date
            max_data_time = max_date
            package_output_path = Path(out_path, site, year, month, package_type)
            package_output_path.mkdir(parents=True, exist_ok=True)
            file_metadata.package_output_path = package_output_path
            file_metadata.path_elements = PathElements(domain=name_parts.domain, site=site, year=year, month=month,
                                                       data_product_id=data_product_id)
            file_metadata.data_product = database.get_data_product(data_product_id)
            file_metadata.manifest_file = ManifestFile(manifest_path, package_type, package_output_path)
            is_first_file = False
        else:
            if min_date < min_data_time:
                min_data_time = min_date
            if max_date > max_data_time:
                max_data_time = max_date
        link_path(path, Path(package_output_path, path.name))
    file_metadata.data_files = DataFiles(files=data_files, min_time=min_data_time, max_time=max_data_time)
    return file_metadata


def get_time_span(path: Path) -> Tuple[str, str]:
    data_frame = pandas.read_csv(path)
    min_start = data_frame.loc[0][0]  # First row, first element is the earliest start date.
    max_end = data_frame.iloc[-1].tolist()[1]  # Last row, second element is the latest end date.
    return min_start, max_end


def link_path(path: Path, link: Path) -> None:
    if not link.exists():
        log.debug(f'Linking file: {path} to: {link}')
        link.symlink_to(path)


def sort_files(in_path: Path, package_type: str) -> Tuple[List[Path], Path]:
    data_files: List[Path] = []
    manifest_file = None
    for path in in_path.rglob('*.csv'):
        if path.is_file():
            filename = path.name
            if filename != ManifestFile.filename and package_type in filename:
                data_files.append(path)
            elif filename == ManifestFile.filename:
                manifest_file = path
    return data_files, manifest_file
