import csv
from datetime import datetime
from pathlib import Path

from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.filename_format import get_filename
from pub_files.publication_workbook import PublicationWorkbook


def write_file(out_path: Path, elements: PathElements, workbook: PublicationWorkbook, timestamp: datetime) -> Path:
    column_names = ['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat']
    filename = get_filename(elements, timestamp=timestamp, file_type='variables', extension='csv')
    path = Path(out_path, filename)
    with open(path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)
        for line in workbook.get_workbook():
            values = []
            for column_name in column_names:
                values.append(line[column_name])
            writer.writerow(values)
    return path
