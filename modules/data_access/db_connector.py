#!/usr/bin/env python3
from typing import Any, Tuple

import structlog
import environs
import psycopg2
from google.cloud.sql.connector import Connector, IPTypes


log = structlog.get_logger()


def read_environment() -> Tuple[str, str, str, str, str]:
    host_key = 'DB_HOST'
    user_key = 'DB_USER'
    password_key = 'DB_PASSWORD'
    name_key = 'DB_NAME'
    schema_key = 'DB_SCHEMA'
    env = environs.Env()
    host = env.str(host_key)
    user = env.str(user_key)
    password = env.str(password_key)
    db = env.str(name_key)
    schema = env.str(schema_key)
    log.debug(f'''
        {host_key}: '{host}'
        {user_key}: '{user}'
        {name_key}: '{db}'
        {schema_key}: '{schema}'
    ''')
    validate(host_key, host)
    validate(user_key, user)
    validate(password_key, password)
    validate(name_key, db)
    validate(schema_key, schema)
    return host, user, password, db, schema


def validate(name, value) -> None:
    if not value:
        raise ValueError(f'Environment variable {name} has not been set.')


class DbConnector:

    def __init__(self) -> None:
        (host, user, password, db, schema) = read_environment()
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.schema = schema
        self.connection = self.__connect()

    def get_schema(self):
        return self.schema

    def get_connection(self):
        return self.connection

    def close(self):
        self.connection.close()

    def __connect(self) -> Any:
        if 'den' in self.host:
            return self.__local_connect()
        else:
            return self.__cloud_connect()

    def __cloud_connect(self) -> Any:
        with Connector() as connector:
            conn = connector.connect(
                self.host,
                driver='pg8000',
                user=self.user,
                password=self.password,
                db=self.db,
                enable_iam_auth=True,
                ip_type=IPTypes.PRIVATE
            )
        return conn

    def __local_connect(self) -> Any:
        return psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            dbname=self.db,
            sslmode='require'
        )
