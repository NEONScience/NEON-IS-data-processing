import os
from typing import NamedTuple, Any

import psycopg2
import structlog
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import pg8000.dbapi


log = structlog.get_logger()


class Db(NamedTuple):
    """Immutable class holds the database connection and the connected schema."""
    connection: Any
    schema: str


class ConnectionParameters(NamedTuple):
    """Immutable class holds the connection parameters."""
    host: str
    user: str
    password: str
    db_name: str
    schema: str


def connect(config_source: str) -> Db:
    if config_source == 'iam':
        return connect_iam_auth()
    elif config_source == 'environment':
        return connect_env()
    else:
        log.error(f'Database config source {config_source} is not "iam" or "environment".')
        exit(1)


def connect_iam_auth() -> Db:
    """
    Uses environment variables to connect to a database in Google Cloud
    using IAM authentication for testing on a local machine. No
    password is required after executing the 'gcloud auth login'
    command. Returns a Google pg8000 based connection.
    """
    pg8000.dbapi.paramstyle = 'pyformat'  # Allows use of psycopg2 named parameter format '%(name)s'.
    connector = Connector(ip_type=IPTypes.PRIVATE, enable_iam_auth=True)
    parameters = read_environment()
    connection = connector.connect(
        parameters.host,
        'pg8000',
        user=parameters.user,
        password=parameters.password,
        db=parameters.db_name
    )
    return Db(connection, parameters.schema)


def connect_env() -> Db:
    """
    Uses environment variables to connect to a database.
    Returns a psycopg2 connection.
    """
    parameters = read_environment()
    connection = psycopg2.connect(
        host=parameters.host,
        port=5432,
        user=parameters.user,
        password=parameters.password,
        dbname=parameters.db_name,
        sslmode='require',
        options=f'-c search_path={parameters.schema}'
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
