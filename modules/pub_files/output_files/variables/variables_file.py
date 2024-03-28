import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import structlog

from pub_files.database.file_variables import FileVariables
from pub_files.database.publication_workbook import PublicationWorkbook, WorkbookRow
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile
from pub_files.output_files.variables.variables_database import VariablesDatabase

log = structlog.get_logger()


def write_file(out_path: Path,
               file_metadata: FileMetadata,
               workbook: PublicationWorkbook,
               timestamp: datetime,
               science_review_file: Optional[ScienceReviewFile],
               database: VariablesDatabase) -> Path:
    """
    Generate and write the variables file into the output path.

    :param out_path: The output path for writing the file.
    :param file_metadata: The metadata from processing the application input files.
    :param workbook: The publication workbook for the data product being published.
    :param timestamp: The timestamp to include in the filename.
    :param science_review_file: An object containing the science review file Path and term names.
    :param database: A class with functions for reading from the database.
    """
    filename = get_filename(file_metadata.path_elements, timestamp=timestamp, file_type='variables', extension='csv')
    print(f'\nfilename:::::\n{filename}\n')
    path = Path(out_path, filename)
    with open(path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat'])
        write_rows(writer, workbook.rows)
        write_sensor_positions_variables(writer, database.get_sensor_position_variables())
        print(f'\nIn variables_file:::::\n{path.read_text()}\n')
        if science_review_file is not None:
            write_science_review_variables(writer, science_review_file.variables)
    return path


def write_rows(writer, rows: List[WorkbookRow]) -> None:
    for row in rows:
        table_name = row.table_name
        field_name = row.field_name
        description = row.description
        data_type = row.data_type_code
        units = row.unit_name
        download_package = row.download_package
        publication_format = row.publication_format
        if download_package != 'none':
            values = [table_name, field_name, description, data_type, units, download_package, publication_format]
            writer.writerow(values)


def write_sensor_positions_variables(writer, file_variables: List[FileVariables]) -> None:
    """
    Read the variables and add them to the variables file.

    :param writer: The file writer.
    :param file_variables: List of sensor position variables.
    """
    for file_variable in file_variables:
        table_name = file_variable.table_name
        description = file_variable.description
        term_name = file_variable.term_name
        download_package = file_variable.download_package
        publication_format = file_variable.publication_format
        data_type = file_variable.data_type
        units = file_variable.units
        row = [table_name, term_name, description, data_type, units, download_package, publication_format]
        writer.writerow(row)


def write_science_review_variables(writer, variables: List[FileVariables]) -> None:
    """Add the science review terms to the variables file."""
    for variable in variables:
        table_name = variable.table_name
        term_name = variable.term_name
        description = variable.description
        data_type = variable.data_type
        units = variable.units
        term_download_package = variable.download_package
        publication_format = variable.publication_format
        row = [table_name, term_name, description, data_type, units, term_download_package, publication_format]
        writer.writerow(row)
