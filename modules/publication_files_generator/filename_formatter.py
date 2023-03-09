"""Functions to standardize file name format."""
from datetime import datetime


def format_timestamp(timestamp: datetime) -> str:
    """Format the timestamp for inclusion in a filename."""
    return timestamp.strftime('%Y%m%dT%H%M%SZ')


def format_filename(*,
                    domain: str,
                    site: str,
                    data_product_id: str,
                    timestamp: datetime,
                    file_type: str,
                    extension: str) -> str:
    """
    This function provides a standard filename format for publication metadata files.

    :param domain: The NEON domain.
    :param site: The site.
    :param data_product_id: The data product ID.
    :param timestamp: The time the file is being created.
    :param file_type: The type of file, e.g. "readme".
    :param extension: The file extension.
    :return: The formatted filename.
    """
    formatted_time = format_timestamp(timestamp)
    return f'NEON.{domain}.{site}.{data_product_id}.{formatted_time}.{file_type}.{extension}'
