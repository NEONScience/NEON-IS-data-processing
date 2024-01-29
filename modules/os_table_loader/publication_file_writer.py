import csv
from calendar import monthrange
from collections import OrderedDict
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import structlog

from os_table_loader.data.data_loader import DataLoader
from os_table_loader.data.result_loader import Result
from os_table_loader.data.result_values_loader import ResultValue
from os_table_loader.publication_date_formatter import format_date

from os_table_loader.publication_number_formatter import format_number
from os_table_loader.publication_string_formatter import format_string
from pub_files.input_files.filename_parser import parse_filename
from pub_files.input_files.manifest_file import ManifestFile

log = structlog.get_logger()


def write_publication_files(*, input_path: Path, workbook_path: Path, out_path: Path,
                            data_loader: DataLoader, file_type: str, partial_table_name: str) -> None:
    """Write a file for each maintenance table."""
    now = datetime.now(timezone.utc)
    time_str = now.strftime('%Y%m%dT%H%M%SZ')
    for path in input_path.rglob('*'):
        if path.is_file():
            metadata_path = Path(*path.parts[2:]).parent
            link_path = Path(out_path, metadata_path, path.name)
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                link_path.symlink_to(path)
            # Process data files only
            if path.name != ManifestFile.get_filename():
                filename_parts = parse_filename(path.name)
                data_product = path.parts[2]
                package_type = filename_parts.package_type
                date = filename_parts.date
                domain = filename_parts.domain
                site = filename_parts.site

                workbook_rows: list[dict] = parse_workbook_file(workbook_path, data_product)

                filename_prefix = f'NEON.{domain}.{site}.{data_product}'
                filename_suffix = f'{date}.{package_type}.{time_str}.{file_type}'

                date_parts = date.split('-')
                year = date_parts[0]
                month = date_parts[1]
                (start_date, end_date) = get_dates(int(year), int(month))

                for table in data_loader.get_tables(partial_table_name):
                    table_workbook_rows = filter_workbook_rows(workbook_rows, table.name, package_type)
                    if not table_workbook_rows:
                        continue
                    results = data_loader.get_site_results(table, site, start_date, end_date)
                    if results:
                        values: dict[Result, list[ResultValue]] = {}
                        for result in results:
                            result_values = data_loader.get_result_values(result)
                            values[result] = list(result_values.values())
                        formatted_table_name = table.name.replace('_pub', '')
                        filename = f'{filename_prefix}.{formatted_table_name}.{filename_suffix}'
                        file_path = Path(out_path, metadata_path, filename)
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        if file_type == 'csv':
                            write_csv(file_path, table_workbook_rows, values)


def write_csv(path: Path, workbook_rows: list[dict],
              result_values: dict[Result, list[ResultValue]]) -> None:
    """Write a CSV file for a maintenance table."""
    formats_by_field_name = get_field_formats(workbook_rows)
    with closing(open(path, 'w', encoding='UTF8')) as file:
        writer = csv.writer(file)
        workbook_field_names = get_workbook_header(workbook_rows)
        writer.writerow(workbook_field_names)
        for result in result_values.keys():
            values = result_values[result]
            values_by_field_name = {}
            for result_value in values:
                values_by_field_name[result_value.field_name] = result_value
            row = []
            for field_name in workbook_field_names:
                publication_format = formats_by_field_name[field_name]
                if field_name == 'uid':
                    row.append(result.result_uuid)
                    continue
                if field_name == 'startDate':
                    row.append(format_date(result.start_date, publication_format))
                    continue
                if field_name == 'endDate':
                    row.append(format_date(result.end_date, publication_format))
                    continue
                try:
                    result_value = values_by_field_name[field_name]
                except KeyError:
                    row.append('')
                    continue
                string_value = result_value.string_value
                number_value = result_value.number_value
                date_value = result_value.date_value
                uri_value = result_value.uri_value
                if string_value is not None:
                    row.append(format_string(string_value, publication_format))
                if number_value is not None:
                    row.append(format_number(number_value, publication_format))
                if date_value is not None:
                    row.append(format_date(date_value, publication_format))
                if uri_value is not None:
                    row.append(uri_value)
            writer.writerow(row)


def get_field_formats(workbook_rows: list[dict]) -> dict[str, str]:
    """Get the publication formats organized by field name."""
    formats_by_field_name = {}
    for row in workbook_rows:
        field_name = row['fieldName']
        field_format = row['pubFormat']
        formats_by_field_name[field_name] = field_format
    return formats_by_field_name


def get_dates(year: int, month: int) -> Tuple[datetime, datetime]:
    """Return the start and end dates for the month."""
    (week_day, day_count) = monthrange(year, month)
    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, day_count)
    return start_date, end_date


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


def filter_workbook_rows(workbook_rows: list[dict], table_name: str, package_type: str) -> list[dict]:
    """Filter the workbook by table name and download package type."""
    filtered_rows = []
    for row in workbook_rows:
        if row['table'] == table_name and row['downloadPkg'] == package_type:
            filtered_rows.append(row)
    return filtered_rows


def get_workbook_header(workbook_rows: list[dict]) -> list[str]:
    """Get the publication workbook header."""
    rows_by_rank: dict[int, str] = {}
    for row in workbook_rows:
        rank = int(row['rank'])
        rows_by_rank[rank] = row['fieldName']
    ordered = OrderedDict(sorted(rows_by_rank.items()))
    return list(ordered.values())
