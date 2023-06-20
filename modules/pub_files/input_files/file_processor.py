from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Dict, NamedTuple

import pandas

import common.date_formatter as date_formatter
from pub_files.database.publication_workbook import PublicationWorkbook, get_file_description
from pub_files.input_files.file_metadata import DataFile, FileMetadata, PathElements, DataFiles
from pub_files.input_files.file_processor_database import FileProcessorDatabase
from pub_files.input_files.filename_parser import parse_filename, FilenameParts
from pub_files.input_files.manifest_file import ManifestFile
from pub_files.input_files.path_parser import parse_path, PathParts


class PublicationPackage(NamedTuple):
    """Contains the publication workbook and the metadata for each package type from reading the input file list."""
    workbook: PublicationWorkbook
    package_metadata: Dict[str, FileMetadata]


def process_files(in_path: Path, out_path: Path, in_path_parse_index: int,
                  database: FileProcessorDatabase) -> PublicationPackage:
    """
    Loop over the input files and extract the needed metadata to product the publication metadata files.

    :param in_path: The input file path.
    :param out_path: The output path for writing files.
    :param in_path_parse_index: The path element index to begin parsing the needed elements from the input path.
    :param database: The database object for retrieving needed data.
    """
    package_metadata: Dict[str, FileMetadata] = {}
    (package_data_files, manifest_path) = sort_files(in_path)
    workbook = None
    is_first_package = True
    for package_type in package_data_files:
        file_metadata = FileMetadata()
        data_files = []
        min_package_time = None
        max_package_time = None
        is_first_file = True
        for path in package_data_files[package_type]:
            path_parts: PathParts = parse_path(path, in_path_parse_index)
            filename_parts: FilenameParts = parse_filename(path.name)
            file_metadata.data_product_id = get_data_product_id(filename_parts)
            if is_first_package and is_first_file:  # only read the publication workbook once
                workbook = database.get_workbook(file_metadata.data_product_id)
                is_first_package = False
            file_description = get_file_description(workbook, filename_parts.table_name, package_type)
            data_files.append(DataFile(filename=path.name,
                                       description=file_description,
                                       line_count=get_file_line_count(path),
                                       data_product_name=filename_parts.data_product_name))
            file_min_time, file_max_time = get_file_time_span(path)
            if is_first_file:
                min_package_time = file_min_time
                max_package_time = file_max_time
                file_metadata.package_output_path = Path(out_path,
                                                         path_parts.site,
                                                         path_parts.year,
                                                         path_parts.month,
                                                         filename_parts.package_type)
                file_metadata.package_output_path.mkdir(parents=True, exist_ok=True)
                file_metadata.path_elements = PathElements(domain=filename_parts.domain,
                                                           site=path_parts.site,
                                                           year=path_parts.year,
                                                           month=path_parts.month,
                                                           data_product_id=file_metadata.data_product_id)
                file_metadata.data_product = database.get_data_product(file_metadata.data_product_id)
                is_first_file = False
            else:
                if file_min_time < min_package_time:
                    min_package_time = file_min_time
                if file_max_time > max_package_time:
                    max_package_time = file_max_time
            link_file(file_metadata.package_output_path, path)
        file_metadata.data_files = DataFiles(files=data_files, min_time=min_package_time, max_time=max_package_time)
        file_metadata.manifest_file = ManifestFile(manifest_path, package_type, file_metadata.package_output_path)
        package_metadata[package_type] = file_metadata
    return PublicationPackage(workbook=workbook, package_metadata=package_metadata)


def get_file_line_count(path: Path) -> int:
    with open(path) as file:
        line_count = sum(1 for line in file) - 1  # subtract header
    return line_count

def get_data_product_id(parts: FilenameParts) -> str:
    """Returns the data product ID in the form stored in the database."""
    return f'NEON.DOM.SITE.{parts.level}.{parts.data_product_number}.{parts.revision}'


def link_file(out_path: Path, file_path: Path) -> None:
    """Link a file into the package output path."""
    link_path = Path(out_path, file_path.name)
    if not link_path.exists():
        link_path.symlink_to(file_path)


def sort_files(in_path: Path) -> Tuple[Dict[str, List[Path]], Path]:
    """Sort the input files by download package type."""
    package_data_files: Dict[str, List[Path]] = {}
    manifest_path = None
    for path in in_path.rglob('*.csv'):
        if path.is_file():
            if path.name != ManifestFile.get_filename():
                name_parts: FilenameParts = parse_filename(path.name)
                try:
                    package_data_files[name_parts.package_type].append(path)
                except KeyError:
                    package_data_files[name_parts.package_type] = [path]
            elif path.name == ManifestFile.get_filename():
                manifest_path = path
    return package_data_files, manifest_path


def get_file_time_span(path: Path) -> Tuple[datetime, datetime]:
    """Return the start and end time for a data file's data."""
    data_frame = pandas.read_csv(path)
    min_time = data_frame.loc[0][0]  # First row, first element is the earliest start time.
    max_time = data_frame.iloc[-1].tolist()[1]  # Last row, second element is the latest end time.
    file_min_time = date_formatter.to_datetime(min_time)
    file_max_time = date_formatter.to_datetime(max_time)
    return file_min_time, file_max_time
