from datetime import datetime
from pathlib import Path
from typing import List

import structlog
from jinja2 import Template

from pub_files.database.geolocation_geometry import Geometry
from pub_files.database.log_entries import LogEntry
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.input_files.file_metadata import PathElements, DataFiles
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.readme.change_log_processor import ChangeLog, get_change_log
from pub_files.output_files.readme.readme_database import ReadmeDatabase

log = structlog.getLogger()


def write_file(readme_template: str,
               out_path: Path,
               file_metadata: FileMetadata,
               timestamp: datetime,
               variables_filename: str,
               positions_filename: str,
               eml_filename: str,
               database: ReadmeDatabase) -> Path:
    elements: PathElements = file_metadata.path_elements
    data_files: DataFiles = file_metadata.data_files
    data_product_id = elements.data_product_id
    keywords: List[str] = database.get_keywords(data_product_id)
    log_entries: List[LogEntry] = database.get_log_entries(data_product_id)
    change_log_entries: List[ChangeLog] = get_change_log(data_product_id, log_entries)
    geometry: Geometry = database.get_geometry(elements.site)
    coordinates: str = geometry.format_coordinates()
    readme_data = dict(timestamp=timestamp,
                       site=elements.site,
                       domain=elements.domain,
                       data_product=file_metadata.data_product,
                       keywords=keywords,
                       data_start_date=data_files.min_time,
                       data_end_date=data_files.max_time,
                       coordinates=coordinates,
                       data_file_count=len(file_metadata.data_files.files),
                       data_files=file_metadata.data_files.files,
                       change_logs=change_log_entries,
                       variables_filename=variables_filename,
                       positions_filename=positions_filename,
                       eml_filename=eml_filename)
    template = Template(readme_template, trim_blocks=True, lstrip_blocks=True)
    content = template.render(readme_data)
    filename = get_filename(elements, timestamp=timestamp, file_type='readme', extension='txt')
    path = Path(out_path, filename)
    path.write_text(content)
    return path
