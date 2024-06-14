import os

import structlog
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import pg8000.dbapi

from errored_datums_reader.db_connector import Db, ConnectionParameters

log = structlog.get_logger()


def connect(parameters: ConnectionParameters) -> Db:
    """
    Uses environment variables to connect to a database in Google Cloud
    using IAM authentication for testing on a local machine. No
    password is required after executing the 'gcloud auth login'
    command. Returns a Google pg8000 based connection.
    """
    pg8000.dbapi.paramstyle = 'pyformat'  # Allows use of psycopg2 named parameter format '%(name)s'.
    connector = Connector()
    connection = connector.connect(
        parameters.host,
        'pg8000',
        ip_type=IPTypes.PRIVATE,
        enable_iam_auth=True,
        user=parameters.user,
        password=parameters.password,
        db=parameters.db_name
    )
    return Db(connection, parameters.schema)


def read_environment() -> ConnectionParameters:
    """Read the database connection parameters from the environment."""
    host = os.environ['DB_HOST']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    db_name = os.environ['DB_NAME']
    schema = os.environ['DB_SCHEMA']
    return ConnectionParameters(
        host=host,
        user=user,
        password=password,
        db_name=db_name,
        schema=schema
    )
