#!/usr/bin/env python3
from contextlib import closing

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
from pub_files.external_files.github_file_reader import GithubFileReader
from pub_files.output_files.eml.eml_file import EmlFile
from pub_files.output_files.sensor_positions_file import SensorPositionsFile
from pub_files.publication_workbook import PublicationWorkbook
from pub_files.timestamp import get_timestamp


def read_github_files(config: ApplicationConfig):
    github_file_reader = GithubFileReader(config)
    eml_files = github_file_reader.get_eml_files()
    publication_workbook = github_file_reader.get_workbook()
    readme_template = github_file_reader.get_readme()
    return eml_files, publication_workbook, readme_template


def main() -> None:
    """Generate the publication metadata files."""
    config = ApplicationConfig()
    in_path = config.in_path
    in_path_parse_index = config.in_path_parse_index
    out_path = config.out_path
    location_path = config.location_path
    log_config.configure(config.log_level)
    (eml_files, workbook, readme_template) = read_github_files(config)
    publication_workbook = PublicationWorkbook(workbook)
    db_config = read_from_mount(config.db_secrets_path)
    with closing(DbConnector(db_config)) as connector:
        file_processor_database = FileProcessorData(connector).get_database()
        sensor_positions_database = SensorPositionsData(connector).get_database()
        eml_database = EmlData(connector).get_database()
        readme_database = ReadmeData(connector).get_database()
        timestamp = get_timestamp()
        metadata = file_processor.process(in_path, out_path, in_path_parse_index,
                                          publication_workbook,
                                          database=file_processor_database)
        elements = metadata.path_elements
        variables_filename = variables_file.write_file(out_path=out_path,
                                                       elements=elements,
                                                       workbook=publication_workbook,
                                                       timestamp=timestamp)
        positions_filename = SensorPositionsFile(location_path=location_path,
                                                 out_path=out_path,
                                                 elements=elements,
                                                 timestamp=timestamp,
                                                 database=sensor_positions_database).write()
        basic_eml_filename = EmlFile(out_path=out_path,
                                     metadata=metadata,
                                     eml_files=eml_files,
                                     publication_workbook=publication_workbook,
                                     package_type='basic',
                                     timestamp=timestamp,
                                     database=eml_database).write()
        expanded_eml_filename = EmlFile(out_path=out_path,
                                        metadata=metadata,
                                        eml_files=eml_files,
                                        publication_workbook=publication_workbook,
                                        package_type='expanded',
                                        timestamp=timestamp,
                                        database=eml_database).write()
        eml_filename = basic_eml_filename  # TODO: Which filename to use, or both?
        readme_file.write_file(out_path=out_path,
                               file_metadata=metadata,
                               readme_template=readme_template,
                               timestamp=timestamp,
                               variables_filename=variables_filename,
                               positions_filename=positions_filename,
                               eml_filename=eml_filename,
                               database=readme_database)


if __name__ == '__main__':
    main()
