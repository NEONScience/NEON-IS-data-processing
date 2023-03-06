import os
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from data_access import db_config_reader
from data_access.db_connector import DbConfig


class DatabaseBackedTest(TestCase):

    def get_config(self) -> DbConfig:
        mount_path = self.configure_mount()
        db_config = db_config_reader.read_from_mount(mount_path)
        return db_config

    def configure_mount(self) -> Path:
        file_keys = db_config_reader.FileKeys
        environment_keys = db_config_reader.EnvironmentKeys
        self.setUpPyfakefs()
        mount_path = Path('/var/db_secret')
        self.fs.create_dir(mount_path)
        self.fs.create_file(Path(mount_path, file_keys.host), contents=os.environ[environment_keys.host])
        self.fs.create_file(Path(mount_path, file_keys.user), contents=os.environ[environment_keys.user])
        self.fs.create_file(Path(mount_path, file_keys.password), contents=os.environ[environment_keys.password])
        self.fs.create_file(Path(mount_path, file_keys.db_name), contents=os.environ[environment_keys.db_name])
        self.fs.create_file(Path(mount_path, file_keys.schema), contents=os.environ[environment_keys.schema])
        return mount_path
