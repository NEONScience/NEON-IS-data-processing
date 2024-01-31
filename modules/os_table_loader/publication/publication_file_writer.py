import csv
from calendar import monthrange
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import structlog

from os_table_loader.data.result_loader import Result
from os_table_loader.data.result_values_loader import ResultValue
from os_table_loader.data.table_loader import Table
from os_table_loader.publication.path_parser import parse_path, PathParts
from os_table_loader.publication.publication_config import PublicationConfig
from os_table_loader.publication.publication_date_formatter import format_date
from os_table_loader.publication.publication_number_formatter import format_number
from os_table_loader.publication.publication_string_formatter import format_string
import os_table_loader.publication.workbook_parser as workbook_parser
from pub_files.input_files.manifest_file import ManifestFile


log = structlog.get_logger()


def write_publication_files(config: PublicationConfig) -> None:
    """Write a file for each maintenance table."""
    now = datetime.now(timezone.utc)
    manifest_files = {}
    new_files = defaultdict(list)
    for path in config.path_config.input_path.rglob('*'):
        if path.is_file():
            path_parts = parse_path(path, config.path_config)
            year = int(path_parts.year)
            month = int(path_parts.month)
            write_file(config.path_config.out_path, path_parts.metadata_path, path)
            if path.name != ManifestFile.get_filename():
                domain = path.name.split('.')[1]
                (start_date, end_date) = get_full_month(year, month)
                workbook_path = config.path_config.workbook_path
                workbook_rows: list[dict] = workbook_parser.parse_workbook_file(workbook_path, path_parts.data_product)
                for table in config.data_loader.get_tables(config.partial_table_name):
                    table_workbook_rows = workbook_parser.filter_workbook_rows(workbook_rows,
                                                                               table.name,
                                                                               path_parts.package_type)
                    if not table_workbook_rows:
                        continue
                    results = config.data_loader.get_site_results(table, path_parts.site, start_date, end_date)
                    if results:
                        values: dict[Result, list[ResultValue]] = {}
                        for result in results:
                            result_values = config.data_loader.get_result_values(result)
                            values[result] = list(result_values.values())
                        filename = get_filename(table, domain, now, path_parts, config)
                        file_path = Path(config.path_config.out_path, path_parts.metadata_path, filename)
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        if config.file_type == 'csv':
                            write_csv(file_path, table_workbook_rows, values)
                            new_files[path_parts.package_type].append(file_path)
            elif path.name == ManifestFile.get_filename():
                output_path = Path(config.path_config.out_path, path_parts.metadata_path)
                output_path.mkdir(parents=True, exist_ok=True)
                manifest_file = ManifestFile(path, path_parts.package_type, output_path)
                manifest_files[path_parts.package_type] = manifest_file
    write_manifests(manifest_files, new_files)


def write_manifests(manifest_files: dict[str, ManifestFile], new_files: defaultdict) -> None:
    """Write any new files to the corresponding manifest file for the package type."""
    for package_type in manifest_files.keys():
        manifest_file = manifest_files[package_type]
        file_paths: list[Path] = new_files[package_type]
        for file_path in file_paths:
            manifest_file.add_file(file_path, has_data=False)
        manifest_file.write_new_manifest()


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


def write_file(out_path: Path, metadata_path: Path, path: Path) -> Path:
    """Link the input file into the output path."""
    link_path = Path(out_path, metadata_path, path.name)
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        link_path.symlink_to(path)
    return metadata_path

def get_full_month(year: int, month: int) -> Tuple[datetime, datetime]:
    """Return the start and end dates for the month."""
    (week_day, day_count) = monthrange(year, month)
    start_date = datetime(year, month, 1)
    end_date = datetime(year, month, day_count)
    return start_date, end_date


def get_filename(table: Table, domain, now: datetime, path_parts: PathParts, config: PublicationConfig):
    table_name = table.name.replace('_pub', '')
    year = path_parts.year
    month = path_parts.month
    site = path_parts.site
    data_product = path_parts.data_product
    package_type = path_parts.package_type
    time = now.strftime('%Y%m%dT%H%M%SZ')
    file_type = config.file_type
    return f'NEON.{domain}.{site}.{data_product}.{table_name}.{year}-{month}.{package_type}.{time}.{file_type}'
