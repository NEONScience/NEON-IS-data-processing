#!/usr/bin/env python3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import pub_files.input_files.file_processor as file_processor
import pub_files.output_files.readme.readme_file as readme_file
import pub_files.output_files.variables_file as variables_file
from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from pub_files.application_config import ApplicationConfig
from pub_files.database.eml_data import EmlData
from pub_files.database.file_processor_data import FileProcessorData
from pub_files.database.readme_data import ReadmeData
from pub_files.database.sensor_positions_data import SensorPositionsData
from pub_files.external_files.file_database import FileDatabase
from pub_files.input_files.file_metadata import PathElements
from pub_files.output_files.eml.eml_file import EmlFile
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles
from pub_files.output_files.sensor_positions.sensor_positions_file import SensorPositionsFile
from pub_files.publication_workbook import PublicationWorkbook


def get_timestamp() -> datetime:
    """Returns the time in UTC."""
    return datetime.now(timezone.utc)


def get_external_files(config: ApplicationConfig) -> Tuple[ExternalEmlFiles, str, str]:
    """Returns the external files needed for publication file generation."""
    file_database = FileDatabase(config)
    eml_files: ExternalEmlFiles = file_database.get_eml_files()
    publication_workbook: str = file_database.get_workbook()
    readme_template: str = file_database.get_readme()
    return eml_files, publication_workbook, readme_template


def get_output_path(out_path: Path, path_elements: PathElements, package_type: str) -> Path:
    output_path = Path(out_path, path_elements.site, path_elements.year, path_elements.month, package_type)
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def main() -> None:
    """Generates the publication metadata files."""
    config = ApplicationConfig()
    in_path = config.in_path
    in_path_parse_index = config.in_path_parse_index
    out_path = config.out_path
    location_path = config.location_path
    log_config.configure(config.log_level)
    db_config = read_from_mount(config.db_secrets_path)
    timestamp = get_timestamp()

    with closing(DbConnector(db_config)) as connector:
        # get all resources
        file_processor_database = FileProcessorData(connector).get_database()
        sensor_positions_database = SensorPositionsData(connector).get_database()
        eml_database = EmlData(connector).get_database()
        readme_database = ReadmeData(connector).get_database()
        (eml_files, workbook, readme_template) = get_external_files(config)
        publication_workbook = PublicationWorkbook(workbook)

        package_types = ['basic', 'expanded']
        for package_type in package_types:
            # process the input files
            metadata = file_processor.process(in_path=in_path,
                                              out_path=out_path,
                                              in_path_parse_index=in_path_parse_index,
                                              package_type=package_type,
                                              publication_workbook=publication_workbook,
                                              database=file_processor_database)
            path_elements = metadata.path_elements
            output_path = get_output_path(out_path, path_elements, package_type)

            # create variables file
            variables_filename = variables_file.write_file(out_path=output_path,
                                                           elements=path_elements,
                                                           workbook=publication_workbook,
                                                           timestamp=timestamp)

            # create sensor positions file
            positions_filename = SensorPositionsFile(location_path=location_path,
                                                     out_path=output_path,
                                                     elements=path_elements,
                                                     timestamp=timestamp,
                                                     database=sensor_positions_database).write()

            # create eml file
            eml_filename = EmlFile(out_path=output_path,
                                   metadata=metadata,
                                   eml_files=eml_files,
                                   publication_workbook=publication_workbook,
                                   package_type='basic',
                                   timestamp=timestamp,
                                   database=eml_database).write()

            # create readme file
            readme_file.write_file(out_path=output_path,
                                   file_metadata=metadata,
                                   readme_template=readme_template,
                                   timestamp=timestamp,
                                   variables_filename=variables_filename,
                                   positions_filename=positions_filename,
                                   eml_filename=eml_filename,
                                   database=readme_database)


if __name__ == '__main__':
    main()
