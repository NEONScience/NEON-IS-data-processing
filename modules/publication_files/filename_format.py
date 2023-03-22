"""Functions to standardize file name."""
from datetime import datetime

from publication_files.file_metadata import PathElements


def format_timestamp(timestamp: datetime) -> str:
    """Format the timestamp for inclusion in a filename."""
    return timestamp.strftime('%Y%m%dT%H%M%SZ')


def get_filename(elements: PathElements, timestamp: datetime, file_type: str, extension: str) -> str:
    """Returns a standard filename for publication metadata files."""
    formatted_timestamp = format_timestamp(timestamp)
    domain = elements.domain
    site = elements.site
    data_product_id = elements.data_product_id
    return f'NEON.{domain}.{site}.{data_product_id}.{formatted_timestamp}.{file_type}.{extension}'
