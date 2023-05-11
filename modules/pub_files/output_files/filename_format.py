"""Functions to standardize output file names."""
from datetime import datetime

from pub_files.input_files.file_metadata import PathElements


def format_timestamp(timestamp: datetime) -> str:
    """Returns a standard timestamp format."""
    return timestamp.strftime('%Y%m%dT%H%M%SZ')


def get_filename(elements: PathElements, timestamp: datetime, file_type: str, extension: str) -> str:
    """
    Returns a standard filename format.

    :param elements: The path elements to include in the filename.
    :param timestamp: The timestamp to include in the filename.
    :param file_type: The file type to include in the filename.
    :param extension: The file extension to include in the filename.
    """
    formatted_timestamp = format_timestamp(timestamp)
    domain = elements.domain
    site = elements.site
    data_product_id = elements.data_product_id.replace('NEON.DOM.SITE.', '')
    return f'NEON.{domain}.{site}.{data_product_id}.{file_type}.{formatted_timestamp}.{extension}'
