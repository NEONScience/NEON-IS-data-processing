import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Callable

from pub_files.database.file_variables import FileVariables
from pub_files.database.science_review_flags import ScienceReviewFlag
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.variables.variables_file_database import VariablesDatabase


# TODO: Indicate which type of product (IS, SAE) in application_config.py
def write_file(file_metadata: FileMetadata,
               package_type: str,
               timestamp: datetime,
               variables_database: VariablesDatabase,
               get_flags: Callable[[str, str], List[ScienceReviewFlag]],
               get_term_name: Callable[[str], str]) -> Path:
    out_path = file_metadata.package_output_path
    path_elements = file_metadata.path_elements
    data_product = file_metadata.data_product
    site = file_metadata.path_elements.site
    filename = get_filename(path_elements, timestamp, 'science_review_flags', 'csv')
    file_path = Path(out_path, filename)
    with open(file_path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        file_variables: List[FileVariables] = variables_database.get_is_science_review()
        file_variables.sort(key=lambda file_variable: file_variable.rank)
        column_names = [x.term_name for x in file_variables if x.download_package == package_type]
        writer.writerow(column_names)
        science_review_flags = get_flags(data_product.data_product_id, site)
        for flag in science_review_flags:
            parts = flag.stream_name.split('.')
            term_number = parts[6]
            horizontal_position = parts[7]
            vertical_position = parts[8]
            term_name = get_term_name(term_number)
            row = [flag.id,
                   to_string(flag.start_date),
                   to_string(flag.end_date),
                   file_metadata.path_elements.domain,
                   file_metadata.path_elements.site,
                   data_product.data_product_id,
                   term_name,
                   horizontal_position,
                   vertical_position,
                   flag.flag,
                   flag.user_comment,
                   to_string(flag.create_date),
                   to_string(flag.last_update)]
            writer.writerow(row)
    return file_path


def to_string(date: datetime) -> Optional[str]:
    """Convert the date to a UTC string."""
    if date is not None:
        return datetime.strftime(date, '%Y-%m-%dT%H:%M:%SZ')
    else:
        return None
