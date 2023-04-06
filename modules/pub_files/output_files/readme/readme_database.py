from typing import NamedTuple, Callable, List

from pub_files.database.queries.log_entries import LogEntry
from pub_files.geometry import Geometry


class ReadmeDatabase(NamedTuple):
    get_geometry: Callable[[str], Geometry]
    get_keywords: Callable[[str], List[str]]
    get_log_entries: Callable[[str], List[LogEntry]]
