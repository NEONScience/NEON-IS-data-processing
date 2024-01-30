import csv
from calendar import monthrange
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import structlog

from os_table_loader.data.result_loader import Result
from os_table_loader.data.result_values_loader import ResultValue
from os_table_loader.publication.publication_config import PublicationConfig
from os_table_loader.publication.publication_date_formatter import format_date
from os_table_loader.publication.publication_number_formatter import format_number
from os_table_loader.publication.publication_string_formatter import format_string
import os_table_loader.publication.workbook_parser as workbook_parser
from pub_files.input_files.filename_parser import parse_filename
from pub_files.input_files.manifest_file import ManifestFile

log = structlog.get_logger()


def write_publication_files(config: PublicationConfig) -> None:
    """Write a file for each maintenance table."""
    now = datetime.now(timezone.utc)
    time_str = now.strftime('%Y%m%dT%H%M%SZ')
    for path in config.input_path.rglob('*'):
        if path.is_file():
            metadata_path = Path(*path.parts[config.input_path_parse_index:]).parent
            link_path = Path(config.out_path, metadata_path, path.name)
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                link_path.symlink_to(path)
            # Process data files only
            if path.name != ManifestFile.get_filename():
                filename_parts = parse_filename(path.name)
                data_product = path.parts[config.data_product_path_index]
                package_type = filename_parts.package_type
                year_month = filename_parts.date
                domain = filename_parts.domain
                site = filename_parts.site

                (start_date, end_date) = get_full_month(year_month)
                workbook_rows: list[dict] = workbook_parser.parse_workbook_file(config.workbook_path, data_product)
                filename_prefix = f'NEON.{domain}.{site}.{data_product}'
                filename_suffix = f'{year_month}.{package_type}.{time_str}.{config.file_type}'

                for table in config.data_loader.get_tables(config.partial_table_name):
                    table_workbook_rows = workbook_parser.filter_workbook_rows(workbook_rows, table.name, package_type)
                    if not table_workbook_rows:
                        continue
                    results = config.data_loader.get_site_results(table, site, start_date, end_date)
                    if results:
                        values: dict[Result, list[ResultValue]] = {}
                        for result in results:
                            result_values = config.data_loader.get_result_values(result)
                            values[result] = list(result_values.values())
                        formatted_table_name = table.name.replace('_pub', '')
                        filename = f'{filename_prefix}.{formatted_table_name}.{filename_suffix}'
                        file_path = Path(config.out_path, metadata_path, filename)
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        if config.file_type == 'csv':
                            write_csv(file_path, table_workbook_rows, values)


def write_csv(path: Path, workbook_rows: list[dict],
              result_values: dict[Result, list[ResultValue]]) -> None:
    """Write a CSV file for a maintenance table."""
    formats_by_field_name = workbook_parser.get_field_formats(workbook_rows)
    with closing(open(path, 'w', encoding='UTF8')) as file:
        writer = csv.writer(file)
        workbook_field_names = workbook_parser.get_workbook_header(workbook_rows)
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


def get_full_month(date: str) -> Tuple[datetime, datetime]:
    """Return the start and end dates for the month."""
    date_parts = date.split('-')
    year = int(date_parts[0])
    month = int(date_parts[1])
    (week_day, day_count) = monthrange(year, month)
    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, day_count)
    return start_date, end_date
