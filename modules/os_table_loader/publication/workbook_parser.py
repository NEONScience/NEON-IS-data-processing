import csv
from collections import OrderedDict
from pathlib import Path


def parse_workbook_file(workbook_path: Path, data_product_idq: str) -> list[dict]:
    """Parse the publication workbook file into a list of dictionaries."""
    expected_filename = f'publication_workbook_NEON.DOM.SITE.{data_product_idq}.txt'
    for path in workbook_path.rglob('*'):
        if path.is_file():
            if path.name == expected_filename:
                with open(path) as file:
                    reader = csv.DictReader(file, delimiter='\t')
                    return list(reader)
    raise SystemExit(f'Publication workbook "{expected_filename}" not found.')


def get_workbook_header(workbook_rows: list[dict]) -> list[str]:
    """Get the publication workbook header."""
    rows_by_rank: dict[int, str] = {}
    for row in workbook_rows:
        rank = int(row['rank'])
        rows_by_rank[rank] = row['fieldName']
    ordered = OrderedDict(sorted(rows_by_rank.items()))
    return list(ordered.values())


def filter_workbook_rows(workbook_rows: list[dict], table_name: str, package_type: str) -> list[dict]:
    """Filter the workbook by table name and download package type."""
    filtered_rows = []
    for row in workbook_rows:
        if row['table'] == table_name and row['downloadPkg'] == package_type:
            filtered_rows.append(row)
    return filtered_rows


def get_field_formats(workbook_rows: list[dict]) -> dict[str, str]:
    """Get the publication formats organized by field name."""
    formats_by_field_name = {}
    for row in workbook_rows:
        field_name = row['fieldName']
        field_format = row['pubFormat']
        formats_by_field_name[field_name] = field_format
    return formats_by_field_name
