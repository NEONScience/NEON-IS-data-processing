import csv
from datetime import datetime
from pathlib import Path
from typing import Callable

from pub_files.database.publication_workbook import PublicationWorkbook
from pub_files.database.term_variables import TermVariables
from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile
from pub_files.output_files.variables.variables_file_database import VariablesDatabase


def write_file(out_path: Path, elements: PathElements, package_type: str, workbook: PublicationWorkbook,
               database: VariablesDatabase, timestamp: datetime, science_review_file: ScienceReviewFile,
               get_term_variables: Callable[[str, str], TermVariables]) -> Path:
    """
    Generate and write the variables file into the output path.

    :param out_path: The output path for writing the file.
    :param elements: The path elements used for generating the filename.
    :param package_type: The download package type.
    :param workbook: The publication workbook for the data product being published.
    :param database: The functions for reading needed data from the database.
    :param timestamp: The timestamp to include in the filename.
    :param science_review_file: An object containing the science review file Path and term names.
    :param get_term_variables: A function accepting a term name string and returning a list of term variables.
    """
    column_names = ['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat']
    filename = get_filename(elements, timestamp=timestamp, file_type='variables', extension='csv')
    path = Path(out_path, filename)
    with open(path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        for row in workbook.workbook_rows:
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
        add_sensor_positions_variables(writer, database)
        add_science_review_variables(writer, science_review_file, package_type, get_term_variables)
    return path


def add_sensor_positions_variables(writer, database: VariablesDatabase) -> None:
    """
    Read the file variables from the database and add them to the file.

    :param writer: The file writer.
    :param database: The functions for reading from the database.
    """
    for file_variable in database.get_sensor_positions():
        table_name = file_variable.table_name
        description = file_variable.description
        term_name = file_variable.term_name
        download_package = file_variable.download_package
        publication_format = file_variable.publication_format
        data_type = file_variable.data_type
        units = file_variable.units
        row = [table_name, term_name, description, data_type, units, download_package, publication_format]
        writer.writerow(row)

def add_science_review_variables(writer, science_review_file: ScienceReviewFile, package_type: str,
                                 get_term_variables: Callable[[str, str], TermVariables]):
    table_name = 'science_review_flags'
    if science_review_file.term_names:
        for term_name in science_review_file.term_names:
            data_product_id = science_review_file.data_product_id
            term_variables = get_term_variables(data_product_id, term_name)
            description = term_variables.description
            data_type = term_variables.data_type
            units = term_variables.units
            download_package = term_variables.download_package
            publication_format = term_variables.publication_format
            if download_package == package_type:
                row = [table_name, term_name, description, data_type, units, download_package, publication_format]
                writer.writerow(row)
