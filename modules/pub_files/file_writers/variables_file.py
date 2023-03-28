import csv
from datetime import datetime
from io import StringIO
from pathlib import Path

from pub_files.input_files.file_metadata import PathElements
from pub_files.file_writers.filename_format import get_filename


def write_file(out_path: Path, elements: PathElements, workbook: str, timestamp: datetime) -> str:
    columns = ['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat']
    filename = get_filename(elements, timestamp=timestamp, file_type='variables', extension='csv')
    path = Path(out_path, elements.site, elements.year, elements.month, filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        reader = csv.DictReader(StringIO(workbook, newline='\n'), delimiter='\t')
        for line in reader:
            values = []
            for name in columns:
                values.append(line[name])
            writer.writerow(values)
    return filename
