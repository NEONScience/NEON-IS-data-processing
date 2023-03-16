#!/usr/bin/env python3
from contextlib import closing
from functools import partial

from common import log_config
from data_access.db_config_reader import read_from_mount
from data_access.db_connector import DbConnector
from publication_files_generator.application_config import read_environment
from publication_files_generator.database_queries.data_product import get_data_product
from publication_files_generator.database_queries.data_product_keywords import get_keywords
from publication_files_generator.database_queries.file_descriptions import get_descriptions
from publication_files_generator.database_queries.geolocation_geometry import get_geometry
from publication_files_generator.database_queries.log_entries import get_log_entries
from publication_files_generator.database_queries.named_location import get_named_location
from publication_files_generator.database_queries.sensor_geolocations import get_geolocations
from publication_files_generator.github_reader import GithubReader
from publication_files_generator.file_processor import process_input_files
from publication_files_generator.sensor_positions_generator import generate_positions_file
from publication_files_generator.readme_generator import generate_readme_file
from publication_files_generator.timestamp import get_timestamp
from publication_files_generator.variables_generator import generate_variables_file


def main() -> None:
    """Generate the publication metadata files."""
    timestamp = get_timestamp()
    config = read_environment()
    reader = GithubReader(pem_file_path=config.github_certificate_path, app_id=config.github_app_id,
                          installation_id=config.github_installation_id, host_url=config.github_host,
                          repo_owner=config.github_repo_owner, branch=config.github_branch)
    publication_workbook = reader.read_file(config.github_publication_workbook_repo,
                                            config.github_publication_workbook_path)
    readme_template = reader.read_file(config.github_readme_repo, config.github_readme_path)

    log_config.configure(config.log_level)
    db_config = read_from_mount(config.db_secrets_path)
    with closing(DbConnector(db_config)) as connector:
        get_descriptions_partial = partial(get_descriptions, connector)
        get_geolocations_partial = partial(get_geolocations, connector)
        get_data_product_partial = partial(get_data_product, connector)
        get_keywords_partial = partial(get_keywords, connector)
        get_geometry_partial = partial(get_geometry, connector)
        get_log_entries_partial = partial(get_log_entries, connector)
        get_named_location_partial = partial(get_named_location, connector)

        file_metadata = process_input_files(config.in_path, config.out_path, config.in_path_parse_index,
                                            get_descriptions_partial)
        variables_filename = generate_variables_file(out_path=config.out_path,
                                                     domain=file_metadata.domain,
                                                     site=file_metadata.site,
                                                     year=file_metadata.year,
                                                     month=file_metadata.month,
                                                     data_product_id=file_metadata.data_product_id,
                                                     publication_workbook=publication_workbook,
                                                     timestamp=timestamp)
        # TODO: Pass this filename to readme.
        positions_filename = generate_positions_file(locations_path=config.locations_path,
                                                     location_path_index=config.location_path_index,
                                                     out_path=config.out_path,
                                                     domain=file_metadata.domain,
                                                     site=file_metadata.site,
                                                     year=file_metadata.year,
                                                     month=file_metadata.month,
                                                     data_product_id=file_metadata.data_product_id,
                                                     timestamp=timestamp,
                                                     get_geolocations=get_geolocations_partial,
                                                     get_named_location=get_named_location_partial,
                                                     get_geometry=get_geometry_partial)
        generate_readme_file(out_path=config.out_path,
                             file_metadata=file_metadata,
                             readme_template=readme_template,
                             timestamp=timestamp,
                             variables_filename=variables_filename,
                             positions_filename=positions_filename,
                             get_data_product=get_data_product_partial,
                             get_geometry=get_geometry_partial,
                             get_keywords=get_keywords_partial,
                             get_log_entries=get_log_entries_partial)


if __name__ == '__main__':
    main()
