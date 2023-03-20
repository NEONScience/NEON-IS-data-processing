"""Functions to standardize file name format."""
from datetime import datetime


def format_timestamp(timestamp: datetime) -> str:
    """Format the timestamp for inclusion in a filename."""
    return timestamp.strftime('%Y%m%dT%H%M%SZ')


def get_filename(*,
                 domain: str,
                 site: str,
                 data_product_id: str,
                 timestamp: datetime,
                 file_type: str,
                 extension: str) -> str:
    """Returns a standard filename format for publication metadata files."""
    formatted_timestamp = format_timestamp(timestamp)
    return f'NEON.{domain}.{site}.{data_product_id}.{formatted_timestamp}.{file_type}.{extension}'
