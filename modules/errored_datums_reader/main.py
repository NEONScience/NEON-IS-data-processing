import os

from pachyderm_sdk import Client

from common import log_config
from errored_datums_reader import db_connector, app
from errored_datums_reader.db_connector import ConnectionParameters


def main() -> None:
    log_level = os.environ['LOG_LEVEL']
    db_host = os.environ['DB_HOST']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']
    db_name = os.environ['DB_NAME']
    db_schema = os.environ['DB_SCHEMA']
    authorization_token = os.environ['AUTHORIZATION_TOKEN']
    log_config.configure(log_level)
    db = db_connector.connect(ConnectionParameters(
        host=db_host,
        user=db_user,
        password=db_password,
        db_name=db_name,
        schema=db_schema
    ))
    client = Client.new_in_cluster(auth_token=authorization_token)
    app.run(client, db)


if __name__ == '__main__':
    main()
