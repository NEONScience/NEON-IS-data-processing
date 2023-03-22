from datetime import datetime
from pathlib import Path
from typing import List, NamedTuple, Callable

import structlog
from jinja2 import Template

from publication_files.change_log import ChangeLog, get_change_log
from publication_files.database_queries.data_product import DataProduct
from publication_files.database_queries.geolocation_geometry import format_coordinates
from publication_files.database_queries.log_entries import LogEntry
from publication_files.file_metadata import PathElements, DataFiles
from publication_files.filename_format import get_filename
from publication_files.file_metadata import FileMetadata

log = structlog.getLogger()


class ReadmeDatabase(NamedTuple):
    get_data_product: Callable[[str], DataProduct]
    get_geometry: Callable[[str], str]
    get_keywords: Callable[[str], List[str]]
    get_log_entries: Callable[[str], List[LogEntry]]


def write_file(readme_template: str,
               out_path: Path,
               file_metadata: FileMetadata,
               timestamp: datetime,
               variables_filename: str,
               positions_filename: str,
               database: ReadmeDatabase) -> None:
    elements: PathElements = file_metadata.path_elements
    data_files: DataFiles = file_metadata.data_files
    data_product_id = elements.data_product_id
    data_product: DataProduct = database.get_data_product(data_product_id)
    keywords: List[str] = database.get_keywords(data_product_id)
    log_entries: List[LogEntry] = database.get_log_entries(data_product_id)
    change_log_entries: List[ChangeLog] = get_change_log(data_product_id, log_entries)
    geometry: str = database.get_geometry(elements.site)
    coordinates: str = format_coordinates(geometry)
    readme_data = dict(timestamp=timestamp,
                       site=elements.site,
                       domain=elements.domain,
                       data_product=data_product,
                       keywords=keywords,
                       data_start_date=data_files.min_time,
                       data_end_date=data_files.max_time,
                       coordinates=coordinates,
                       data_file_count=len(file_metadata.data_files.files),
                       data_files=file_metadata.data_files.files,
                       change_logs=change_log_entries,
                       variables_filename=variables_filename,
                       positions_filename=positions_filename)
    readme_filename = get_filename(elements, timestamp=timestamp, file_type='readme', extension='txt')
    readme_path = Path(out_path, elements.site, elements.year, elements.month, readme_filename)
    template = Template(readme_template, trim_blocks=True, lstrip_blocks=True)
    readme_content = template.render(readme_data)
    readme_path.write_text(readme_content)
    log.debug(f'Readme path: {readme_path}')
    print(f'\nReadme content:\n{readme_content}\n')
