import environs
from pachyderm_sdk import Client

from common import log_config
from processed_datums_reader import db_connector, app
from processed_datums_reader.db_connector import ConnectionParameters


def main() -> None:
    env = environs.Env()
    authorization_token = env.str('AUTHORIZATION_TOKEN')
    db_host = env.str('DB_HOST')
    db_name = env.str('DB_NAME')
    db_password = env.str('DB_PASSWORD')
    db_schema = env.str('DB_SCHEMA')
    db_user = env.str('DB_USER')
    log_level = env.log_level('LOG_LEVEL')
    l1_pipelines_path: Path = env.path('PIPELINE_NAME_L1', None)
    log_config.configure(log_level)
    db = db_connector.connect(ConnectionParameters(
        host=db_host,
        user=db_user,
        password=db_password,
        db_name=db_name,
        schema=db_schema
    ))
    client = Client.new_in_cluster(auth_token=authorization_token)
    app.run(client, db, l1_pipelines_path)


if __name__ == '__main__':
    main()
