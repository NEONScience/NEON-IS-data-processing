from datetime import datetime
from io import StringIO
import csv
from pathlib import Path

from publication_files_generator.filename_formatter import format_filename


def generate_variables_file(out_path: Path,
                            domain: str,
                            site: str,
                            year: str,
                            month: str,
                            data_product_id: str,
                            publication_workbook: str,
                            timestamp: datetime) -> str:
    column_names = ['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat']
    reader = csv.DictReader(StringIO(publication_workbook, newline='\n'), delimiter='\t')
    rows = f"{','.join(column_names)}\n"
    for line in reader:
        row = ''
        values = []
        for name in column_names:
            values.append(line[name])
        row += f"{','.join(values)}\n"
        rows += row
    variables_filename = format_filename(domain=domain, site=site, data_product_id=data_product_id,
                                         timestamp=timestamp, file_type='variables', extension='csv')
    variables_dir_path = Path(out_path, site, year, month)
    variables_dir_path.mkdir(parents=True, exist_ok=True)
    Path(variables_dir_path, variables_filename).write_text(rows)
    return variables_filename
