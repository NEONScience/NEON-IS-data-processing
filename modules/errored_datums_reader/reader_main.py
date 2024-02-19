from contextlib import closing

import environs
import structlog
from marshmallow.validate import OneOf
from pachyderm_sdk import Client

from common import log_config
from errored_datums_reader import db_connector
from errored_datums_reader.reader import read_error_files
from errored_datums_reader.writer import write_to_db


def main() -> None:
    env = environs.Env()
    log_level = env.str('LOG_LEVEL')
    db_config_source = env.str('DB_CONFIG_SOURCE',
                               validate=OneOf(
                                    ['iam', 'environment'],
                                    error='DB_CONFIG_SOURCE must be one of: {choices}'))
    client_source = env.str('CLIENT_SOURCE',
                            validate=OneOf(
                                ['config', 'cluster'],
                                error='CLIENT_SOURCE must be one of: {choices}'))
    authorization_token = env.str('AUTHORIZATION_TOKEN')
    log_config.configure(log_level)
    log = structlog.get_logger()
    db = db_connector.connect(db_config_source)
    if client_source == 'config':
        client = Client.from_config()
    elif client_source == 'cluster':
        client = Client.new_in_cluster(auth_token=authorization_token)
    else:
        log.error(f'Pachyderm client source {client_source} is not "config" or "cluster".')
        exit(1)
    files_by_pipeline = read_error_files(client)
    with closing(db.connection):
        write_to_db(db, files_by_pipeline)


if __name__ == '__main__':
    main()
