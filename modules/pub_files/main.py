#!/usr/bin/env python3
from contextlib import closing

import pub_files.file_writers.readme.readme_file as readme_file
import pub_files.file_writers.variables_file as variables_file
import pub_files.input_files.file_processor as file_processor
from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from pub_files.config import ApplicationConfig
from pub_files.database import Database
from pub_files.file_writers.eml_file import EmlFile
from pub_files.file_writers.sensor_positions_file import SensorPositionsFile
from pub_files.github_files.github_file_reader import GithubFileReader
from pub_files.timestamp import get_timestamp


def main() -> None:
    """Generate the publication metadata files."""
    config = ApplicationConfig()
    in_path = config.in_path
    in_path_parse_index = config.in_path_parse_index
    out_path = config.out_path
    location_path = config.location_path
    log_level = config.log_level
    db_secrets_path = config.db_secrets_path

    log_config.configure(log_level)
    github_reader = GithubFileReader(config)
    db_config = read_from_mount(db_secrets_path)
    with closing(DbConnector(db_config)) as connector:
        database = Database(connector)
        timestamp = get_timestamp()
        metadata = file_processor.process(in_path, out_path, in_path_parse_index,
                                          database=database.file_processor())
        elements = metadata.path_elements
        variables_filename = variables_file.write_file(out_path=out_path,
                                                       elements=elements,
                                                       workbook=github_reader.get_workbook(),
                                                       timestamp=timestamp)
        positions_filename = SensorPositionsFile(location_path=location_path,
                                                 out_path=out_path,
                                                 elements=elements,
                                                 timestamp=timestamp,
                                                 database=database.sensor_positions()).write()
        eml_filename = EmlFile(out_path=out_path,
                               metadata=metadata,
                               boilerplate=github_reader.get_eml(),
                               timestamp=timestamp,
                               database=database.eml()).write()
        readme_file.write_file(out_path=out_path,
                               file_metadata=metadata,
                               readme_template=github_reader.get_readme(),
                               timestamp=timestamp,
                               variables_filename=variables_filename,
                               positions_filename=positions_filename,
                               eml_filename=eml_filename,
                               database=database.readme())


if __name__ == '__main__':
    main()
