from pathlib import Path

import structlog

from os_table_loader.table_data import FieldValue
from os_table_loader.table_loader import Table


log = structlog.get_logger()


def get_filename(table: Table, extension: str) -> str:
    # data_product = table.source_data_product
    # parts = data_product.split('.')
    # parts[1] = domain
    # parts[2] = site
    # full_data_product_name = '.'.join(parts)
    file_name = f'{table.data_product}.{table.name}.{extension}'
    log.debug(f'file_name: {file_name}')
    return file_name


def get_filepath(out_path, file_name) -> Path:
    # year = result.start_date.strftime('%Y')
    # month = result.start_date.strftime('%m')
    # root_path = Path(out_path, site, year, month)
    out_path.mkdir(parents=True, exist_ok=True)
    file_path = Path(out_path, file_name)
    return file_path


def get_domain(field_values: list[FieldValue]) -> str:
    for field_value in field_values:
        if field_value.field.field_name == 'domainID':
            return field_value.value.string_value


def get_site(field_values: list[FieldValue]) -> str:
    for field_value in field_values:
        if field_value.field.field_name == 'siteID':
            return field_value.value.string_value
