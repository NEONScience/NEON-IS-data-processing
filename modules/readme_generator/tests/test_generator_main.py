#!/usr/bin/env python3
import logging
import os
import unittest
from pathlib import Path

import environs

import readme_generator.generator_main as readme_generator_main
from data_access import db_config_reader

log = logging.getLogger()


def add_secrets(db_secrets_path) -> None:
    file_keys = db_config_reader.FileKeys
    environment_keys = db_config_reader.EnvironmentKeys
    host = Path(db_secrets_path, file_keys.host)
    host.write_text(os.environ[environment_keys.host])
    user = Path(db_secrets_path, file_keys.user)
    user.write_text(os.environ[environment_keys.user])
    password = Path(db_secrets_path, file_keys.password)
    password.write_text(os.environ[environment_keys.password])
    db_name = Path(db_secrets_path, file_keys.db_name)
    db_name.write_text(os.environ[environment_keys.db_name])
    schema = Path(db_secrets_path, file_keys.schema)
    schema.write_text(os.environ[environment_keys.schema])


class ReadmeGeneratorMainTest(unittest.TestCase):

    def setUp(self) -> None:
        self.test_files_path = Path(os.path.dirname(__file__), 'generator_main_test_files')
        self.out_path = Path(os.path.dirname(__file__), 'output')
        self.out_path.mkdir(parents=False, exist_ok=True)
        self.db_secrets_path = Path(os.path.dirname(__file__), 'db_secrets')
        self.db_secrets_path.mkdir(parents=False, exist_ok=True)
        add_secrets(self.db_secrets_path)

    # @unittest.skip('Integration test skipped.')
    def test_main(self) -> None:
        # TODO: Specify individual path indices and pass them into path parser.
        pem_path = environs.Env().str('GITHUB_README_APP_PEM')
        os.environ['IN_PATH'] = str(self.test_files_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['DB_SECRETS_PATH'] = str(self.db_secrets_path)
        os.environ['PATH_PARSE_INDEX'] = '8'
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['GITHUB_PEM'] = pem_path
        os.environ['GITHUB_APP_ID'] = '300002'
        os.environ['GITHUB_INSTALLATION_ID'] = '34765458'
        os.environ['GITHUB_HOST'] = 'https://api.github.com'
        os.environ['GITHUB_REPO_OWNER'] = 'NEONScience'
        os.environ['GITHUB_REPO'] = 'neon-metadata-docs'
        os.environ['GITHUB_README_TEMPLATE_PATH'] = 'readme/readmeTemplate.txt'
        os.environ['GITHUB_BRANCH'] = ''
        readme_generator_main.main()

    def tearDown(self) -> None:
        """Remove the created directories from the filesystem."""
        # self.out_path.rmdir()
        for path in self.db_secrets_path.glob('*'):
            path.unlink(missing_ok=True)
        self.db_secrets_path.rmdir()
        pass
