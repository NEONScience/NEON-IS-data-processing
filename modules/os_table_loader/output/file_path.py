from pathlib import Path

from os_table_loader.table_data import FieldValue
from os_table_loader.data.table_loader import Table


def get_filename(*, table: Table, extension: str) -> str:
    # data_product = table.source_data_product
    # parts = data_product.split('.')
    # parts[1] = domain
    # parts[2] = site
    # full_data_product_name = '.'.join(parts)
    return f'{table.data_product}.{table.name}.{extension}'


def get_filepath(out_path: Path, file_name: str) -> Path:
    # year = result.start_date.strftime('%Y')
    # month = result.start_date.strftime('%m')
    # root_path = Path(out_path, site, year, month)
    out_path.mkdir(parents=True, exist_ok=True)
    return Path(out_path, file_name)


def get_domain(field_values: list[FieldValue]) -> str:
    for field_value in field_values:
        if field_value.field.field_name == 'domainID':
            return field_value.value.string_value


def get_site(field_values: list[FieldValue]) -> str:
    for field_value in field_values:
        if field_value.field.field_name == 'siteID':
            return field_value.value.string_value
