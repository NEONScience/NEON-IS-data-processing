#!/usr/bin/env python3
from contextlib import closing

from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from publication_files.application_config import ApplicationConfig
from publication_files.database import get_database
from publication_files.timestamp import get_timestamp
import publication_files.file_processor as file_processor
import publication_files.file_generators.sensor_positions_file as sensor_file
import publication_files.file_generators.readme_file as readme_file
import publication_files.file_generators.variables_file as variables_file


def main() -> None:
    """Generate the publication metadata files."""
    config = ApplicationConfig()
    log_config.configure(config.log_level)
    db_config = read_from_mount(config.db_secrets_path)
    with closing(DbConnector(db_config)) as connector:
        database = get_database(connector)
        timestamp = get_timestamp()
        file_metadata = file_processor.process(config.in_path, config.out_path, config.in_path_parse_index,
                                               database=database.file_processor_database)
        elements = file_metadata.path_elements
        variables_filename = variables_file.write_file(config.out_path, elements, config.get_workbook(), timestamp)
        positions_filename = sensor_file.write_file(location_path=config.location_path,
                                                    out_path=config.out_path,
                                                    elements=elements,
                                                    timestamp=timestamp,
                                                    database=database.sensor_positions_database)
        readme_file.write_file(out_path=config.out_path,
                               file_metadata=file_metadata,
                               readme_template=config.get_readme(),
                               timestamp=timestamp,
                               variables_filename=variables_filename,
                               positions_filename=positions_filename,
                               database=database.readme_database)


if __name__ == '__main__':
    main()
