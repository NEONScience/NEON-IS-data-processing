from datetime import datetime
from pathlib import Path
from typing import List

import structlog
from jinja2 import Template

from publication_files_generator.change_log import ChangeLog, get_change_log
from publication_files_generator.data_store import DataStore
from publication_files_generator.database_queries.data_product import DataProduct
from publication_files_generator.database_queries.location_geometry import get_point_coordinates
from publication_files_generator.database_queries.log_entries import LogEntry
from publication_files_generator.filename_formatter import format_filename
from publication_files_generator.file_processor import InputFileMetadata

log = structlog.getLogger()


def generate_readme_file(*,
                         out_path: Path,
                         data_store: DataStore,
                         input_file_metadata: InputFileMetadata,
                         readme_template: str,
                         timestamp: datetime,
                         variables_filename: str) -> None:
    data_product_id = input_file_metadata.data_product_id
    domain = input_file_metadata.domain
    site = input_file_metadata.site
    year = input_file_metadata.year
    month = input_file_metadata.month
    data_files = input_file_metadata.data_files
    min_time = input_file_metadata.min_time
    max_time = input_file_metadata.max_time

    data_product: DataProduct = data_store.get_data_product(data_product_id)
    keywords: List[str] = data_store.get_keywords(data_product_id)
    log_entries: List[LogEntry] = data_store.get_log_entries(data_product_id)
    change_log_entries: List[ChangeLog] = get_change_log(data_product_id, log_entries)
    geometry: str = data_store.get_geometry(site)
    coordinates: str = get_point_coordinates(geometry)
    data_file_count: int = len(data_files)
    readme_data = dict(timestamp=timestamp,
                       site=site,
                       domain=domain,
                       data_product=data_product,
                       keywords=keywords,
                       data_start_date=min_time,
                       data_end_date=max_time,
                       coordinates=coordinates,
                       data_file_count=data_file_count,
                       data_files=data_files,
                       change_logs=change_log_entries,
                       variables_filename=variables_filename)
    template = Template(readme_template, trim_blocks=True, lstrip_blocks=True)
    readme_content = template.render(readme_data)
    readme_filename = format_filename(domain=domain,
                                      site=site,
                                      data_product_id=data_product.short_data_product_id,
                                      timestamp=timestamp,
                                      file_type='readme',
                                      extension='txt')
    readme_path = Path(out_path, site, year, month, readme_filename)
    readme_path.write_text(readme_content)

    log.debug(f'Readme path: {readme_path}')
    log.debug(f'\nReadme content:\n{readme_content}\n')
