import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

from pub_files.data_product import get_data_product_number
from pub_files.database.file_variables import FileVariables
from pub_files.database.science_review_flags import ScienceReviewFlag
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review_database import ScienceReviewDatabase
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile, Term



# TODO: Only IS format is implemented, how to programmatically match data to columns for each science type?
# TODO: Query data product type from dp_catalog (IS, SAE, etc.) to configure which file format to use.
def write_file(file_metadata: FileMetadata, package_type: str, timestamp: datetime,
               database: ScienceReviewDatabase) -> Optional[ScienceReviewFile]:
    flags = read_flags(file_metadata, database)
    if not flags:
        return None
    terms: list[Term] = []
    file_path = get_file_path(file_metadata, timestamp)
    variables = database.get_variables()
    header = get_header(variables, package_type)
    with open(file_path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        keys = []
        for flag in flags:
            key = get_key(flag)
            if key in keys:  # only add the first flag for the same stream name, date range, and flag values
                continue
            else:
                keys.append(key)
                parts = flag.stream_name.split('.')
                term_number = parts[6]
                horizontal_position = parts[7]
                vertical_position = parts[8]
                term_name = database.get_term_name(term_number)
                term = Term(name=term_name, number=term_number)
                terms.append(term)
                row = get_row(flag, term, horizontal_position, vertical_position, file_metadata)
                writer.writerow(row)
    return ScienceReviewFile(path=file_path,
                             data_product_id=file_metadata.data_product.data_product_id,
                             variables=variables)


def read_flags(metadata: FileMetadata, database: ScienceReviewDatabase) -> list[ScienceReviewFlag]:
    path_elements = metadata.path_elements
    data_start_date = metadata.data_files.min_time
    data_end_date = metadata.data_files.max_time
    data_product_number = get_data_product_number(metadata.data_product.data_product_id)
    return database.get_flags(data_product_number, path_elements.site, data_start_date, data_end_date)


def get_file_path(metadata: FileMetadata, timestamp: datetime) -> Path:
    filename = get_filename(metadata.path_elements, timestamp, 'science_review_flags', 'csv')
    return Path(metadata.package_output_path, filename)


def get_key(flag: ScienceReviewFlag) -> str:
    stream = flag.stream_name[:-4]  # drop ending '.tmi' from stream name
    start = format_date(flag.start_date)
    end = format_date(flag.end_date)
    return f'{stream}_{start}_{end}_{flag.flag}'


def get_header(file_variables: list[FileVariables], package_type: str) -> list[str]:
    file_variables.sort(key=lambda file_variable: file_variable.rank)
    column_names = [x.term_name for x in file_variables if x.download_package == package_type]
    return column_names


def get_row(flag: ScienceReviewFlag, term: Term, horizontal_position: str, vertical_position: str,
            metadata: FileMetadata):
    return [flag.id,
            format_date(flag.start_date),
            format_date(flag.end_date),
            metadata.path_elements.domain,
            metadata.path_elements.site,
            get_data_product_number(metadata.data_product.data_product_id),
            term.name,
            horizontal_position,
            vertical_position,
            flag.flag,
            flag.user_comment,
            format_date(flag.create_date),
            format_date(flag.last_update)]


def format_date(date: datetime) -> Optional[str]:
    """Convert the date to UTC string format."""
    if date is not None:
        return datetime.strftime(date, '%Y-%m-%dT%H:%M:%SZ')
    return None
