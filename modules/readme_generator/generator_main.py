#!/usr/bin/env python3
from contextlib import closing

import readme_generator.github_app_reader as github_app_reader
from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from readme_generator.generator_main_config import GeneratorMainConfig
from readme_generator.database import Database
from readme_generator.generator import generate_readme, DataFunctions, Paths


def main() -> None:
    """Entry point to generate a readme file."""
    config = GeneratorMainConfig()
    log_config.configure(config.log_level)
    readme_template: str = github_app_reader.read_file(config.get_github_config())
    db_config = read_from_mount(config.db_secrets_path)
    with closing(DbConnector(db_config)) as connector:
        paths: Paths = config.get_paths()
        data_functions: DataFunctions = Database(connector).get_data_functions()
        generate_readme(paths, data_functions, readme_template)


if __name__ == '__main__':
    main()
