from contextlib import closing
from pathlib import Path

import environs
import structlog
from marshmallow.validate import OneOf
from pachyderm_sdk import Client

from common import log_config
from data_access.db_config_reader import read_from_mount, read_from_environment
from data_access.db_connector import DbConnector
from errored_datums_reader.reader import read_error_files
from errored_datums_reader.writer import write_results


def get_local_client() -> Client:
    """Connects using the local config at ~/.pachyderm/config.json"""
    return Client.from_config()


def get_cluster_client(authorization_token: str) -> Client:
    """Connects from within a Pachyderm cluster."""
    return Client.new_in_cluster(auth_token=authorization_token)


def main() -> None:
    env = environs.Env()
    log_level = env.str('LOG_LEVEL')
    db_config_source = env.str('DB_CONFIG_SOURCE',
                               validate=OneOf(['mount', 'environment'],
                               error='DB_CONFIG_SOURCE must be one of: {choices}'))
    authorization_token = env.str('AUTHORIZATION_TOKEN')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug('Running.')
    if db_config_source == 'environment':
        db_config = read_from_environment()
        client = get_local_client()
    elif db_config_source == 'mount':
        db_config = read_from_mount(Path('/var/db_secret'))
        client = get_cluster_client(authorization_token)
    else:
        log.error('Invalid database config source.')
        exit(1)
    with closing(DbConnector(db_config)) as connector:
        version = client.get_version()
        log.debug(f'\n version: {version}\n')
        paths_by_repo = read_error_files(client)
        write_results(connector, paths_by_repo)


if __name__ == '__main__':
    main()
