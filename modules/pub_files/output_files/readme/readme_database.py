from functools import partial
from typing import NamedTuple, Callable, List

from data_access.db_connector import DbConnector
from pub_files.database.data_product_keywords import get_keywords
from pub_files.database.geolocation_geometry import get_geometry
from pub_files.database.log_entries import LogEntry
from pub_files.database.log_entries import get_log_entries
from pub_files.geometry import Geometry


class ReadmeDatabase(NamedTuple):
    """Class to consolidate the functions to read the needed data for the readme file from the database."""
    get_geometry: Callable[[str], Geometry]
    get_keywords: Callable[[str], List[str]]
    get_log_entries: Callable[[str], List[LogEntry]]


def get_readme_database(connector: DbConnector) -> ReadmeDatabase:
    return ReadmeDatabase(get_geometry=partial(get_geometry, connector),
                          get_keywords=partial(get_keywords, connector),
                          get_log_entries=partial(get_log_entries, connector))
