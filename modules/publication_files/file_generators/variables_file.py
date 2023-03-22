import csv
from datetime import datetime
from io import StringIO
from pathlib import Path

from publication_files.file_metadata import PathElements
from publication_files.filename_format import get_filename


def write_file(out_path: Path, elements: PathElements, workbook: str, timestamp: datetime) -> str:
    columns = ['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat']
    filename = get_filename(elements, timestamp=timestamp, file_type='variables', extension='csv')
    root = Path(out_path, elements.site, elements.year, elements.month)
    root.mkdir(parents=True, exist_ok=True)
    with open(Path(root, filename), 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        reader = csv.DictReader(StringIO(workbook, newline='\n'), delimiter='\t')
        for line in reader:
            values = []
            for name in columns:
                values.append(line[name])
            writer.writerow(values)
    return filename
