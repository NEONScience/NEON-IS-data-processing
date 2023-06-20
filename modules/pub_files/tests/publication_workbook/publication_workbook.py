import csv
import os
from pathlib import Path
from typing import Dict, List

from pub_files.database.publication_workbook import PublicationWorkbook, WorkbookRow


def get_workbook(_data_product_id) -> PublicationWorkbook:
    file_descriptions: Dict[str, str] = {}
    workbook_rows: List[WorkbookRow] = []
    file_path = Path(os.path.dirname(__file__), 'soil_temperature_publication_workbook.txt')
    with open(file_path) as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in list(reader):
            table_name = row['table']
            field_name = row['fieldName']
            description = row['description']
            data_type_code = row['dataType']
            measurement_scale = row['measurementScale']
            publication_format = row['pubFormat']
            download_package = row['downloadPkg']
            unit_name = row['units']
            lov_code = row['lovName']
            table_description = row['tableDescription']
            file_descriptions[f'{table_name}.{download_package}'] = table_description
            workbook_row = WorkbookRow(table_name=table_name,
                                       field_name=field_name,
                                       description=description,
                                       data_type_code=data_type_code,
                                       measurement_scale=measurement_scale,
                                       publication_format=publication_format,
                                       download_package=download_package,
                                       unit_name=unit_name,
                                       lov_code=lov_code,
                                       table_description=table_description)
            workbook_rows.append(workbook_row)
    return PublicationWorkbook(rows=workbook_rows, file_descriptions=file_descriptions)
