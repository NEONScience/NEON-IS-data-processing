"""Module for abstracting data source."""
from typing import NamedTuple, Callable, List, Dict

from publication_files_generator.database_queries.data_product import DataProduct
from publication_files_generator.database_queries.log_entries import LogEntry


class DataStore(NamedTuple):
    """Interface for data reading functions."""
    get_log_entries: Callable[[str], List[LogEntry]]
    get_data_product: Callable[[str], DataProduct]
    get_geometry: Callable[[str], str]
    get_descriptions: Callable[[], Dict[str, str]]
    get_keywords: Callable[[str], List[str]]
