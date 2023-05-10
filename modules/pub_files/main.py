#!/usr/bin/env python3
from contextlib import closing
from datetime import datetime, timezone
from typing import Tuple

import pub_files.output_files.readme.readme_file as readme_file
import pub_files.output_files.variables.variables_file as variables_file
from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from pub_files.application_config import ApplicationConfig
from pub_files.external_files.external_files import ExternalFiles
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.input_files.file_processor import process_files, PublicationPackage
from pub_files.input_files.file_processor_database import get_file_processor_database
from pub_files.output_files.eml.eml_database import get_eml_database
from pub_files.output_files.eml.eml_file import EmlFile
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles
from pub_files.output_files.readme.readme_database import get_readme_database
from pub_files.output_files.sensor_positions.sensor_positions_database import get_sensor_positions_database
from pub_files.output_files.sensor_positions.sensor_positions_file import SensorPositionsFile
from pub_files.output_files.variables.variables_file_database import get_variables_database


def get_timestamp() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(timezone.utc)


def get_external_files(config: ApplicationConfig) -> Tuple[ExternalEmlFiles, str]:
    """Return the external files needed for publication file generation."""
    external_files = ExternalFiles(config)
    eml_files: ExternalEmlFiles = external_files.get_eml_files()
    readme_template: str = external_files.get_readme()
    return eml_files, readme_template


def main() -> None:
    """Generate the publication metadata files."""
    config = ApplicationConfig()
    log_config.configure(config.log_level)
    db_config = read_from_mount(config.db_secrets_path)
    timestamp = get_timestamp()
    with closing(DbConnector(db_config)) as connector:
        file_processor_database = get_file_processor_database(connector)
        variables_database = get_variables_database(connector)
        sensor_positions_database = get_sensor_positions_database(connector)
        eml_database = get_eml_database(connector)
        readme_database = get_readme_database(connector)
        (eml_files, readme_template) = get_external_files(config)
        # get required metadata from the input files and link the files into the output path
        publication_package: PublicationPackage = process_files(in_path=config.in_path,
                                                                out_path=config.out_path,
                                                                in_path_parse_index=config.in_path_parse_index,
                                                                database=file_processor_database)
        publication_workbook = publication_package.workbook
        for package_type in publication_package.package_metadata:
            file_metadata: FileMetadata = publication_package.package_metadata[package_type]
            # write variables file
            variables_path = variables_file.write_file(out_path=file_metadata.package_output_path,
                                                       elements=file_metadata.path_elements,
                                                       workbook=publication_workbook,
                                                       database=variables_database,
                                                       timestamp=timestamp)
            # write sensor positions file
            positions_path = SensorPositionsFile(location_path=config.location_path,
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
