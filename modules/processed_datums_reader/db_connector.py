from typing import NamedTuple, Any

import psycopg2
import structlog


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


def connect(parameters: ConnectionParameters) -> Db:
    """
    Uses environment variables to connect to a database.
    Returns a psycopg2 connection.
    """
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
