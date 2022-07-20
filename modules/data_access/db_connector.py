import psycopg2
from psycopg2.extensions import parse_dsn, connection


def connect(db_url: str) -> connection:
    """
    Return a database connection based on the input URL.

    :param db_url: The database URL.
    :returns: The connection.
    """
    db_params = parse_dsn(db_url)
    return psycopg2.connect(**db_params)
