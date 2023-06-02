import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Callable

import structlog

from pub_files.database.file_variables import FileVariables
from pub_files.database.science_review_flags import ScienceReviewFlag
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.variables.variables_file_database import VariablesDatabase


log = structlog.get_logger()


# TODO: Indicate which type of product (IS, SAE) in application_config.py
def write_file(file_metadata: FileMetadata,
               package_type: str,
               timestamp: datetime,
               variables_database: VariablesDatabase,
               get_flags: Callable[[str, str], List[ScienceReviewFlag]],
               get_term_name: Callable[[str], str]) -> Path:
    path_elements = file_metadata.path_elements
    data_product = file_metadata.data_product
    filename = get_filename(path_elements, timestamp, 'science_review_flags', 'csv')
    file_path = Path(file_metadata.package_output_path, filename)
    with open(file_path, 'w', encoding='UTF8', newline='') as file:
        flags = get_flags(data_product.data_product_id, path_elements.site)
        if not flags:
            log.debug('No science review flags found, writing message to file.')
            file.write('No science review flags are present for this data product.\n')
        else:
            log.debug('Writing science review file header.')
            column_names = get_column_names(variables_database, package_type)
            writer = csv.writer(file)
            writer.writerow(column_names)
            for flag in flags:
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
                log.debug('Writing row to science review file.')
                writer.writerow(row)
    return file_path


def get_column_names(variables_database: VariablesDatabase, package_type: str) -> List[str]:
    # TODO: Only read file header values from database once, and pass them in.
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
