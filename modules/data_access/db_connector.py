#!/usr/bin/env python3
from typing import Any
from typing import NamedTuple

import psycopg2


class DbConfig(NamedTuple):
    host: str
    user: str
    password: str
    database_name: str
    schema: str


class DbConnector:

    def __init__(self, config: DbConfig) -> None:
        self.config = config
        self.connection = self._connect()

    def get_schema(self):
        return self.config.schema

    def get_connection(self):
        return self.connection

    def close(self):
        self.connection.close()

    def _connect(self) -> Any:
        return psycopg2.connect(
            host=self.config.host,
            port=5432,
            user=self.config.user,
            password=self.config.password,
            dbname=self.config.database_name,
            sslmode='require',
            options=f'-c search_path={self.config.schema}'
        )
