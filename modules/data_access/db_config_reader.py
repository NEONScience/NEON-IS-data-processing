import environs
from pathlib import Path
from typing import NamedTuple

from data_access.db_connector import DbConfig, DbConnector


class FileKeys:
    host = 'hostname'
    user = 'username'
    password = 'password'
    db_name = 'database'
    schema = 'schema'


class EnvironmentKeys(NamedTuple):
    host = 'DB_HOST'
    user = 'DB_USER'
    password = 'DB_PASSWORD'
    db_name = 'DB_NAME'
    schema = 'DB_SCHEMA'


def read_from_mount(mount_path: Path) -> DbConfig:
    host_file = Path(mount_path, FileKeys.host)
    user_file = Path(mount_path, FileKeys.user)
    password_file = Path(mount_path, FileKeys.password)
    database_name_file = Path(mount_path, FileKeys.db_name)
    schema_file = Path(mount_path, FileKeys.schema)
    with open(host_file) as f:
        host = f.readline()
    with open(user_file) as f:
        user = f.readline()
    with open(password_file) as f:
        password = f.readline()
    with open(database_name_file) as f:
        database_name = f.readline()
    with open(schema_file) as f:
        schema = f.readline()
    return DbConfig(host=host, user=user, password=password, database_name=database_name, schema=schema)


def read_from_environment() -> DbConfig:
    env = environs.Env()
    host = env.str(EnvironmentKeys.host)
    user = env.str(EnvironmentKeys.user)
    password = env.str(EnvironmentKeys.password)
    database_name = env.str(EnvironmentKeys.db_name)
    schema = env.str(EnvironmentKeys.schema)
    validate(EnvironmentKeys.host, host)
    validate(EnvironmentKeys.user, user)
    validate(EnvironmentKeys.password, password)
    validate(EnvironmentKeys.db_name, database_name)
    validate(EnvironmentKeys.schema, schema)
    return DbConfig(host=host, user=user, password=password, database_name=database_name, schema=schema)


def validate(name, value) -> None:
    if not value:
        raise ValueError(f'Environment variable {name} has not been set.')


def get_connector(config_source: str) -> DbConnector:
    """Return a database connector for the given config string."""
    if config_source == 'mount':
        mount_path = Path('/var/db_secret')
        db_config = read_from_mount(mount_path)
        return DbConnector(db_config)
    elif config_source == 'environment':
        db_config = read_from_environment()
        return DbConnector(db_config)
