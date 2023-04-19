#!/usr/bin/env python3
from contextlib import closing
from datetime import datetime, timezone
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
from pub_files.external_files.external_files import ExternalFiles
from pub_files.output_files.eml.eml_file import EmlFile
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles
from pub_files.output_files.sensor_positions.sensor_positions_file import SensorPositionsFile
from pub_files.publication_workbook import PublicationWorkbook


def get_timestamp() -> datetime:
    """Returns the current time in UTC."""
    return datetime.now(timezone.utc)


def get_external_files(config: ApplicationConfig) -> Tuple[ExternalEmlFiles, str, str]:
    """Returns the external files needed for publication file generation."""
    external_files = ExternalFiles(config)
    eml_files: ExternalEmlFiles = external_files.get_eml_files()
    publication_workbook: str = external_files.get_workbook()
    readme_template: str = external_files.get_readme()
    return eml_files, publication_workbook, readme_template


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

        for package_type in ['basic', 'expanded']:
            # get required metadata from the input files and link them into the output directory
            file_metadata = file_processor.process(in_path=in_path,
                                                   out_path=out_path,
                                                   in_path_parse_index=in_path_parse_index,
                                                   package_type=package_type,
                                                   workbook=publication_workbook,
                                                   database=file_processor_database)

            # write variables file
            variables_path = variables_file.write_file(out_path=file_metadata.package_output_path,
                                                       elements=file_metadata.path_elements,
                                                       workbook=publication_workbook,
                                                       timestamp=timestamp)

            # write sensor positions file
            positions_path = SensorPositionsFile(location_path=location_path,
                                                 out_path=file_metadata.package_output_path,
                                                 elements=file_metadata.path_elements,
                                                 timestamp=timestamp,
                                                 database=sensor_positions_database).write()

            # write eml file
            eml_path = EmlFile(out_path=file_metadata.package_output_path,
                               file_metadata=file_metadata,
                               eml_files=eml_files,
                               publication_workbook=publication_workbook,
                               package_type=package_type,
                               timestamp=timestamp,
                               database=eml_database).write()

            # write readme file
            readme_path = readme_file.write_file(out_path=file_metadata.package_output_path,
                                                 file_metadata=file_metadata,
                                                 readme_template=readme_template,
                                                 timestamp=timestamp,
                                                 variables_filename=variables_path.name,
                                                 positions_filename=positions_path.name,
                                                 eml_filename=eml_path.name,
                                                 database=readme_database)

            # update and write the manifest
            file_metadata.manifest_file.add_metadata_files(variables_file=variables_path,
                                                           positions_file=positions_path,
                                                           eml_file=eml_path,
                                                           readme_file=readme_path)
            file_metadata.manifest_file.write()


if __name__ == '__main__':
    main()
