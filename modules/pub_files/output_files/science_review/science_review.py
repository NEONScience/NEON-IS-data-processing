import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Callable

from pub_files.data_product import get_data_product_number
from pub_files.database.file_variables import FileVariables
from pub_files.database.science_review_flags import ScienceReviewFlag
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile, Term
from pub_files.output_files.variables.variables_file_database import VariablesDatabase


# TODO: Only IS format is implemented, how to programmatically match data to columns for each science type?
# TODO: Query data product type from dp_catalog (IS, SAE, etc.) to configure which file format to use.
def write_file(file_metadata: FileMetadata, package_type: str, timestamp: datetime,
               variables_database: VariablesDatabase,
               get_flags: Callable[[str, str, datetime, datetime], List[ScienceReviewFlag]],
               get_term_name: Callable[[str], str]) -> ScienceReviewFile:
    path_elements = file_metadata.path_elements
    data_product = file_metadata.data_product
    data_start_date = file_metadata.data_files.min_time
    data_end_date = file_metadata.data_files.max_time
    data_product_number = get_data_product_number(data_product.data_product_id)
    flags = get_flags(data_product_number, path_elements.site, data_start_date, data_end_date)
    if not flags:
        return ScienceReviewFile(None, None, None)
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    keys = []
    rows = []
    terms: List[Term] = []
    for flag in flags:
        parts = flag.stream_name.split('.')
        print(f'parts: {parts}')
        term_number = parts[6]
        horizontal_position = parts[7]
        vertical_position = parts[8]
        term_name = get_term_name(term_number)
        stream_name_without_temporal_index = flag.stream_name[:-4]
        start_date = flag.start_date.strftime(date_format)
        end_date = flag.end_date.strftime(date_format)
        key = f'{stream_name_without_temporal_index}_{start_date}_{end_date}_{flag.flag}'
        print(f'key: {key}')
        if key in keys:  # only add first flag for the stream, date range, and flag value
            continue
        else:
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
        terms.append(Term(name=term_name, number=term_number))
    if rows:
        filename = get_filename(path_elements, timestamp, 'science_review_flags', 'csv')
        file_path = Path(file_metadata.package_output_path, filename)
        with open(file_path, 'w', encoding='UTF8', newline='') as file:
            column_names = get_column_names(variables_database, package_type)
            writer = csv.writer(file)
            writer.writerow(column_names)
            writer.writerows(rows)
        return ScienceReviewFile(file_path, data_product.data_product_id, terms)


def get_column_names(variables_database: VariablesDatabase, package_type: str) -> List[str]:
    # TODO: Only read file variables from the database once, and pass them into the write_file() function above.
    file_variables: List[FileVariables] = variables_database.get_is_science_review()
    file_variables.sort(key=lambda file_variable: file_variable.rank)
    column_names = [x.term_name for x in file_variables if x.download_package == package_type]
    return column_names


def to_string(date: datetime) -> Optional[str]:
    """Convert the date to the expected UTC string format."""
    if date is not None:
        return datetime.strftime(date, '%Y-%m-%dT%H:%M:%SZ')
    else:
        return None
