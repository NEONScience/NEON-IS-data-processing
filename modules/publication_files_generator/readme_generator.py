from datetime import datetime
from pathlib import Path
from typing import List, Callable

import structlog
from jinja2 import Template

from publication_files_generator.change_log import ChangeLog, get_change_log
from publication_files_generator.database_queries.data_product import DataProduct
from publication_files_generator.database_queries.geolocation_geometry import get_formatted_coordinates
from publication_files_generator.database_queries.log_entries import LogEntry
from publication_files_generator.filename_formatter import get_filename
from publication_files_generator.file_processor import InputFileMetadata

log = structlog.getLogger()


def generate_readme_file(readme_template: str,
                         out_path: Path,
                         file_metadata: InputFileMetadata,
                         timestamp: datetime,
                         variables_filename: str,
                         positions_filename: str,
                         get_log_entries: Callable[[str], List[LogEntry]],
                         get_data_product: Callable[[str], DataProduct],
                         get_geometry: Callable[[str], str],
                         get_keywords: Callable[[str], List[str]]) -> None:
    data_product_id = file_metadata.data_product_id
    data_product: DataProduct = get_data_product(data_product_id)
    print(f'\n\ndata_product: {data_product}\n\n')
    keywords: List[str] = get_keywords(data_product_id)
    log_entries: List[LogEntry] = get_log_entries(data_product_id)
    change_log_entries: List[ChangeLog] = get_change_log(data_product_id, log_entries)
    geometry: str = get_geometry(file_metadata.site)
    coordinates: str = get_formatted_coordinates(geometry)
    readme_data = dict(timestamp=timestamp,
                       site=file_metadata.site,
                       domain=file_metadata.domain,
                       data_product=data_product,
                       keywords=keywords,
                       data_start_date=file_metadata.min_time,
                       data_end_date=file_metadata.max_time,
                       coordinates=coordinates,
                       data_file_count=len(file_metadata.data_files),
                       data_files=file_metadata.data_files,
                       change_logs=change_log_entries,
                       variables_filename=variables_filename,
                       positions_filename=positions_filename)
    readme_filename = get_filename(domain=file_metadata.domain, site=file_metadata.site,
                                   data_product_id=data_product.short_data_product_id,
                                   timestamp=timestamp, file_type='readme', extension='txt')
    readme_path = Path(out_path, file_metadata.site, file_metadata.year, file_metadata.month, readme_filename)
    template = Template(readme_template, trim_blocks=True, lstrip_blocks=True)
    readme_content = template.render(readme_data)
    readme_path.write_text(readme_content)
    log.debug(f'Readme path: {readme_path}')
    log.debug(f'\nReadme content:\n{readme_content}\n')
