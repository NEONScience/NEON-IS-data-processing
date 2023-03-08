#!/usr/bin/env python3
from contextlib import closing

from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from publication_files_generator import github_reader
from publication_files_generator.config import read_environment, Config
from publication_files_generator.database import get_data_store
from publication_files_generator.github_reader import GithubConfig
from publication_files_generator.file_processor import process_input_files
from publication_files_generator.readme_generator import generate_readme_file
from publication_files_generator.timestamp import get_timestamp
from publication_files_generator.variables_generator import generate_variables_file


def get_github_config(config: Config) -> GithubConfig:
    return GithubConfig(pem_file_path=config.github_certificate_path,
                        app_id=config.github_app_id,
                        installation_id=config.github_installation_id,
                        host_url=config.github_host,
                        owner=config.github_repo_owner,
                        branch=config.github_branch)


def main() -> None:
    config = read_environment()
    log_config.configure(config.log_level)
    db_config = read_from_mount(config.db_secrets_path)
    with closing(DbConnector(db_config)) as connector:
        data_store = get_data_store(connector)
        timestamp = get_timestamp()
        input_file_metadata = process_input_files(config.in_path,
                                                  config.out_path,
                                                  config.in_path_parse_index,
                                                  data_store)
        read_file = github_reader.get_read_file(get_github_config(config))
        readme_template = read_file(config.github_readme_repo, config.github_readme_path)
        publication_workbook = read_file(config.github_publication_workbook_repo,
                                         config.github_publication_workbook_path)
        variables_filename = generate_variables_file(out_path=config.out_path,
                                                     domain=input_file_metadata.domain,
                                                     site=input_file_metadata.site,
                                                     year=input_file_metadata.year,
                                                     month=input_file_metadata.month,
                                                     data_product_id=input_file_metadata.data_product_id,
                                                     publication_workbook=publication_workbook,
                                                     timestamp=timestamp)
        generate_readme_file(out_path=config.out_path,
                             data_store=data_store,
                             input_file_metadata=input_file_metadata,
                             readme_template=readme_template,
                             timestamp=timestamp,
                             variables_filename=variables_filename)


if __name__ == '__main__':
    main()
