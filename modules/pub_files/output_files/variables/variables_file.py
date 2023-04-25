import csv
from datetime import datetime
from pathlib import Path

from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.variables.variables_file_database import VariablesDatabase
from pub_files.publication_workbook import PublicationWorkbook


def write_file(out_path: Path, elements: PathElements, workbook: PublicationWorkbook, database: VariablesDatabase,
               timestamp: datetime) -> Path:
    column_names = ['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat']
    filename = get_filename(elements, timestamp=timestamp, file_type='variables', extension='csv')
    path = Path(out_path, filename)
    with open(path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        for line in workbook.get_workbook():
            package = line['downloadPkg']
            if package != 'none':
                values = []
                for column_name in column_names:
                    values.append(line[column_name])
                writer.writerow(values)
        add_sensor_positions_variables(writer, database)
    return path


def add_sensor_positions_variables(writer, database: VariablesDatabase) -> None:
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
