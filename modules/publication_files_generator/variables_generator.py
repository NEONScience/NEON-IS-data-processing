import csv
from datetime import datetime
from io import StringIO
from pathlib import Path

from publication_files_generator.application_config import ApplicationConfig
from publication_files_generator.file_processor import InputFileMetadata
from publication_files_generator.filename_formatter import get_filename
from publication_files_generator.github_reader import GithubReader


def run(config: ApplicationConfig, reader: GithubReader, metadata: InputFileMetadata, timestamp: datetime) -> str:
    publication_workbook = reader.read_file(config.github_publication_workbook_repo,
                                            config.github_publication_workbook_path)
    return write_file(config.out_path,
                      metadata.domain,
                      metadata.site,
                      metadata.year,
                      metadata.month,
                      metadata.data_product_id,
                      publication_workbook,
                      timestamp)


def write_file(out_path: Path,
               domain: str,
               site: str,
               year: str,
               month: str,
               data_product_id: str,
               publication_workbook: str,
               timestamp: datetime) -> str:
    rows = make_rows(publication_workbook)
    filename = get_filename(domain=domain, site=site, data_product_id=data_product_id,
                            timestamp=timestamp, file_type='variables', extension='csv')
    root = Path(out_path, site, year, month)
    root.mkdir(parents=True, exist_ok=True)
    Path(root, filename).write_text(rows)
    return filename


def make_rows(publication_workbook: str) -> str:
    columns = ['table', 'fieldName', 'description', 'dataType', 'units', 'downloadPkg', 'pubFormat']
    reader = csv.DictReader(StringIO(publication_workbook, newline='\n'), delimiter='\t')
    rows = f"{','.join(columns)}\n"
    for line in reader:
        row = ''
        values = []
        for name in columns:
            values.append(line[name])
        row += f"{','.join(values)}\n"
        rows += row
    return rows
