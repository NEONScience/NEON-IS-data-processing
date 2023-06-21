from datetime import datetime
from pathlib import Path
from typing import List, Optional

from jinja2 import Template

from pub_files.database.geolocation_geometry import Geometry
from pub_files.database.log_entries import LogEntry
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.input_files.file_metadata import DataFiles
from pub_files.output_files.filename_format import get_filename
from pub_files.output_files.readme.change_log_processor import ChangeLog, get_change_log
from pub_files.output_files.readme.readme_database import ReadmeDatabase
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile


def write_file(readme_template: str, out_path: Path, file_metadata: FileMetadata, timestamp: datetime,
               variables_filename: str, positions_filename: str, eml_filename: str,
               science_review_file: Optional[ScienceReviewFile], database: ReadmeDatabase) -> Path:
    """
    Create and write to the output path a publication metadata readme file using the given template.

    :param readme_template: The readme Jinja template.
    :param out_path: The root path for writing the file.
    :param file_metadata: The metadata derived by processing the input files into the application.
    :param timestamp: The timestamp to include in the readme filename.
    :param variables_filename: The variables filename to include in the readme file.
    :param positions_filename: The sensor positions filename to include in the readme file.
    :param eml_filename: The EML filename to include in the readme file.
    :param science_review_file: The science review object containing filename to include in the readme file.
    :param database: The object for reading needed data from the database.
    """
    data_files: DataFiles = file_metadata.data_files
    data_product_id = file_metadata.path_elements.data_product_id
    keywords: List[str] = database.get_keywords(data_product_id)
    log_entries: List[LogEntry] = database.get_log_entries(data_product_id)
    change_log_entries: List[ChangeLog] = get_change_log(data_product_id, log_entries)
    geometry: Geometry = database.get_geometry(file_metadata.path_elements.site)
    coordinates: str = geometry.formatted_coordinates
    readme_data = dict(timestamp=timestamp,
                       site=file_metadata.path_elements.site,
                       domain=file_metadata.path_elements.domain,
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
    if science_review_file is not None:
        readme_data['science_review_filename'] = science_review_file.path.name
    else:
        readme_data['science_review_filename'] = None
    template = Template(readme_template, trim_blocks=True, lstrip_blocks=True)
    content = template.render(readme_data)
    filename = get_filename(file_metadata.path_elements, timestamp=timestamp, file_type='readme', extension='txt')
    path = Path(out_path, filename)
    path.write_text(content)
    return path
