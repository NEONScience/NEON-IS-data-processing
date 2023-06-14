#!/usr/bin/env python3
import os
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import pub_files.output_files.readme.readme_file as readme_file
import pub_files.output_files.variables.variables_file as variables_file
from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from pub_files.application_config import ApplicationConfig
from pub_files.external_files.external_files import get_external_files
from pub_files.input_files.file_metadata import FileMetadata
from pub_files.input_files.file_processor import process_files, PublicationPackage
from pub_files.input_files.file_processor_database import get_file_processor_database
from pub_files.input_files.path_parser import parse_path, PathParts
from pub_files.output_files.eml.eml_database import get_eml_database
from pub_files.output_files.eml.eml_file import EmlFile
from pub_files.output_files.readme.readme_database import get_readme_database
from pub_files.output_files.science_review.science_review import write_file as write_science_review_file
from pub_files.output_files.science_review.science_review_database import get_science_review_database
from pub_files.output_files.sensor_positions.sensor_positions_database import get_sensor_positions_database
from pub_files.output_files.sensor_positions.sensor_positions_file import write_file as write_sensor_positions_file
from pub_files.output_files.variables.variables_database import get_variables_database


def main() -> None:
    """Generate the publication metadata files."""
    config = ApplicationConfig()
    log_config.configure(config.log_level)
    db_config = read_from_mount(config.db_secrets_path)
    timestamp = get_timestamp()

    with closing(DbConnector(db_config)) as connector:
        file_processor_database = get_file_processor_database(connector)
        sensor_positions_database = get_sensor_positions_database(connector)
        eml_database = get_eml_database(connector)
        readme_database = get_readme_database(connector)
        science_review_database = get_science_review_database(connector)
        variables_database = get_variables_database(connector)
        external_files = get_external_files(config)

        # Get to each site-year-month to send into the processor, starting at the parse index
        location_base_path = config.location_path.parts[0:config.in_path_parse_index + 1]
        for root, dirs, files in os.walk(config.in_path):
            datum_path = Path(root)
            if len(datum_path.parts) == (config.in_path_parse_index + 4):
                # get required metadata from the input files and link files into the output path
                publication_package: PublicationPackage = process_files(in_path=datum_path,
                                                                        out_path=config.out_path,
                                                                        in_path_parse_index=config.in_path_parse_index,
                                                                        database=file_processor_database)
                for package_type in publication_package.package_metadata:
                    file_metadata: FileMetadata = publication_package.package_metadata[package_type]
                    # write sensor positions file
                    location_path = get_location_path(datum_path, location_base_path, config.in_path_parse_index)
                    positions_path = write_sensor_positions_file(location_path=location_path,
                                                                 out_path=file_metadata.package_output_path,
                                                                 elements=file_metadata.path_elements,
                                                                 timestamp=timestamp,
                                                                 database=sensor_positions_database)
                    # write eml file
                    eml_path = EmlFile(out_path=file_metadata.package_output_path,
                                       file_metadata=file_metadata,
                                       eml_files=external_files.eml_files,
                                       publication_workbook=publication_package.workbook,
                                       package_type=package_type,
                                       timestamp=timestamp,
                                       database=eml_database).write()
                    # write science review file
                    science_review_file = write_science_review_file(file_metadata=file_metadata,
                                                                    package_type=package_type,
                                                                    timestamp=timestamp,
                                                                    database=science_review_database)
                    # write variables file
                    variables_path = variables_file.write_file(out_path=file_metadata.package_output_path,
                                                               file_metadata=file_metadata,
                                                               workbook=publication_package.workbook,
                                                               timestamp=timestamp,
                                                               science_review_file=science_review_file,
                                                               database=variables_database)
                    # write readme file
                    readme_path = readme_file.write_file(out_path=file_metadata.package_output_path,
                                                         file_metadata=file_metadata,
                                                         readme_template=external_files.readme_template,
                                                         timestamp=timestamp,
                                                         variables_filename=variables_path.name,
                                                         positions_filename=positions_path.name,
                                                         eml_filename=eml_path.name,
                                                         science_review_file=science_review_file,
                                                         database=readme_database)
                    # write new manifest
                    file_metadata.manifest_file.add_metadata_files(variables_file=variables_path,
                                                                   positions_file=positions_path,
                                                                   eml_file=eml_path,
                                                                   readme_file=readme_path,
                                                                   science_review_file=science_review_file)
                    file_metadata.manifest_file.write_new_manifest()


def get_timestamp() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(timezone.utc)


def get_location_path(datum_path: Path, location_base_path: Tuple[str], path_parse_index: int) -> Path:
    """Returns a path to this particular site-year-month datum."""
    datum_parts: PathParts = parse_path(datum_path, path_parse_index)
    location_path = Path(*location_base_path, datum_parts.year, datum_parts.month, '01', datum_parts.site)
    return location_path


if __name__ == '__main__':
    main()
