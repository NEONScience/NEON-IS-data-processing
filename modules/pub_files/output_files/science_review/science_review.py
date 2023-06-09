import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Callable

import structlog

from pub_files.data_product import get_data_product_number
from pub_files.database.file_variables import FileVariables
from pub_files.database.science_review_flags import ScienceReviewFlag
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile, Term


log = structlog.get_logger()


# TODO: Only IS format is implemented, how to programmatically match data to columns for each science type?
# TODO: Query data product type from dp_catalog (IS, SAE, etc.) to configure which file format to use.
def write_file(file_metadata: FileMetadata, package_type: str, timestamp: datetime,
               get_file_variables: Callable[[], List[FileVariables]],
               get_flags: Callable[[str, str, datetime, datetime], List[ScienceReviewFlag]],
               get_term_name: Callable[[str], str]) -> ScienceReviewFile:
    path_elements = file_metadata.path_elements
    data_product = file_metadata.data_product
    data_start_date = file_metadata.data_files.min_time
    data_end_date = file_metadata.data_files.max_time
    data_product_number = get_data_product_number(data_product.data_product_id)
    flags = get_flags(data_product_number, path_elements.site, data_start_date, data_end_date)
    if not flags:
        return ScienceReviewFile(None, None, [])
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    keys: List[str] = []
    rows = []
    terms: List[Term] = []
    for flag in flags:
        parts = flag.stream_name.split('.')
        log.debug(f'parts: {parts}')
        term_number = parts[6]
        horizontal_position = parts[7]
        vertical_position = parts[8]
        term_name = get_term_name(term_number)
        stream_name_without_temporal_index = flag.stream_name[:-4]
        start_date = flag.start_date.strftime(date_format)
        end_date = flag.end_date.strftime(date_format)
        key = f'{stream_name_without_temporal_index}_{start_date}_{end_date}_{flag.flag}'
        log.debug(f'key: {key} package: {package_type}')
        if key in keys:  # only add first flag for the stream, date range, and flag value
            log.debug(f'key is in keys')
            continue
        else:
            log.debug(f'key is new')
            keys.append(key)
            row = [flag.id,
                   to_string(flag.start_date),
                   to_string(flag.end_date),
                   file_metadata.path_elements.domain,
                   file_metadata.path_elements.site,
                   data_product_number,
                   term_name,
                   horizontal_position,
                   vertical_position,
                   flag.flag,
                   flag.user_comment,
                   to_string(flag.create_date),
                   to_string(flag.last_update)]
            rows.append(row)
            log.debug(f'Adding term: {term_name}')
            terms.append(Term(name=term_name, number=term_number))
    if rows:
        filename = get_filename(path_elements, timestamp, 'science_review_flags', 'csv')
        file_path = Path(file_metadata.package_output_path, filename)
        with open(file_path, 'w', encoding='UTF8', newline='') as file:
            column_names = get_column_names(get_file_variables, package_type)
            writer = csv.writer(file)
            writer.writerow(column_names)
            writer.writerows(rows)
        return ScienceReviewFile(file_path, data_product.data_product_id, terms)
    else:
        return ScienceReviewFile(None, None, [])


def get_column_names(get_file_variables: Callable[[], List[FileVariables]], package_type: str) -> List[str]:
    file_variables: List[FileVariables] = get_file_variables()
    file_variables.sort(key=lambda file_variable: file_variable.rank)
    column_names = [x.term_name for x in file_variables if x.download_package == package_type]
    return column_names


def to_string(date: datetime) -> Optional[str]:
    """Convert the date to UTC string format."""
    if date is not None:
        return datetime.strftime(date, '%Y-%m-%dT%H:%M:%SZ')
    else:
        return None
