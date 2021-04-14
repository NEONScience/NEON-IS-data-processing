from typing import Any

import psycopg2
from psycopg2.extensions import parse_dsn


def connect(db_url: str) -> Any:
    """
    Return a database connection based on the input database URL.

    @param db_url: The database URL.
    @returns: The connection.
    """
    db_params = parse_dsn(db_url)
    return psycopg2.connect(**db_params)
