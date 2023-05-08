from typing import NamedTuple


class FilenameParts(NamedTuple):
    """The required data from the filename."""
    domain: str
    site: str
    level: str
    data_product_number: str
    revision: str
    horizontal_index: str
    vertical_index: str
    temporal_index: str
    table_name: str
    date: str
    package_type: str


def parse_filename(filename: str) -> FilenameParts:
    """Parses the filename of a data file to extract the needed elements."""
    parts = filename.split('.')
    domain = parts[1]
    site = parts[2]
    level = parts[3]
    data_product_number = parts[4]
    revision = parts[5]
    horizontal_index = parts[6]
    vertical_index = parts[7]
    temporal_index = parts[8]
    table_name = parts[9]
    date = parts[10]
    package_type = parts[11]
    return FilenameParts(domain=domain,
                         site=site,
                         level=level,
                         data_product_number=data_product_number,
                         revision=revision,
                         horizontal_index=horizontal_index,
                         vertical_index=vertical_index,
                         temporal_index=temporal_index,
                         table_name=table_name,
                         date=date,
                         package_type=package_type)
