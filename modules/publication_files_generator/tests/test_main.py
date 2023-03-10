#!/usr/bin/env python3
import logging
import os
import unittest
from pathlib import Path

import environs

from publication_files_generator.main import main
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


@unittest.skip('Integration test skipped.')
class MainTest(unittest.TestCase):

    def setUp(self) -> None:
        self.test_files_path = Path(os.path.dirname(__file__), 'main_test_files')
        self.out_path = Path(os.path.dirname(__file__), 'output')
        self.out_path.mkdir(parents=False, exist_ok=True)
        self.db_secrets_path = Path(os.path.dirname(__file__), 'db_secrets')
        self.db_secrets_path.mkdir(parents=False, exist_ok=True)
        add_secrets(self.db_secrets_path)
        self.site_path = Path(self.out_path, 'CPER')
        self.year_path = Path(self.site_path, '2020')
        self.month_path = Path(self.year_path, '01')
        self.day_path = Path(self.month_path, '02')

    def test_main(self) -> None:
        # TODO: Specify individual path indices and pass them into path parser.
        pem_path = environs.Env().str('GITHUB_README_APP_PEM')
        os.environ['IN_PATH'] = str(self.test_files_path)
        os.environ['IN_PATH_PARSE_INDEX'] = '8'
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['DB_SECRETS_PATH'] = str(self.db_secrets_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['GITHUB_PEM'] = pem_path
        os.environ['GITHUB_APP_ID'] = '300002'
        os.environ['GITHUB_INSTALLATION_ID'] = '34765458'
        os.environ['GITHUB_HOST'] = 'https://api.github.com'
        os.environ['GITHUB_REPO_OWNER'] = 'NEONScience'
        os.environ['GITHUB_README_REPO'] = 'neon-metadata-docs'
        os.environ['GITHUB_README_PATH'] = 'readme/readmeTemplate.txt'
        os.environ['GITHUB_PUBLICATION_WORKBOOK_REPO'] = 'landWaterSoilIPT'
        os.environ['GITHUB_PUBLICATION_WORKBOOK_PATH'] = 'water_quality/PublicationWorkbook_Water_quality.txt'
        os.environ['GITHUB_BRANCH'] = ''
        main()
        readme_count = len(list(self.month_path.glob('*.txt')))
        assert readme_count == 1
        csv_file_count = len(list(self.month_path.glob('*.csv')))
        assert csv_file_count == 2  # includes variables and sensor_positions
        data_files_count = len(list(self.day_path.glob('*.csv')))
        assert data_files_count == 5  # includes 4 data files and manifest.csv file

    def tearDown(self) -> None:
        """Remove the created files and directories from the filesystem."""
        self.remove_directories()
        self.remove_database_secrets()

    def remove_directories(self):
        # delete data files and manifest
        for path in self.day_path.glob('*'):
            if path.is_file():
                path.unlink(missing_ok=True)
        # delete monthly files
        for path in self.month_path.glob('*'):
            if path.is_file():
                path.unlink(missing_ok=True)
        self.day_path.rmdir()
        self.month_path.rmdir()
        self.year_path.rmdir()
        self.site_path.rmdir()
        self.out_path.rmdir()

    def remove_database_secrets(self):
        for path in self.db_secrets_path.glob('*'):
            path.unlink(missing_ok=True)
        self.db_secrets_path.rmdir()

