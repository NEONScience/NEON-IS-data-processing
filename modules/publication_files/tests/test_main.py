#!/usr/bin/env python3
import logging
import os
import unittest
from pathlib import Path

import environs

from publication_files.main import main
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
        root = os.path.dirname(__file__)
        self.data_path = Path(root, 'main_test_files', 'data')
        self.location_path = Path(root, 'main_test_files', 'locations', 'CPER', '2020', '01', '02', 'location')
        self.out_path = Path(root, 'output')
        self.out_path.mkdir(parents=False, exist_ok=True)
        self.db_secrets_path = Path(root, 'db_secrets')
        self.db_secrets_path.mkdir(parents=False, exist_ok=True)
        add_secrets(self.db_secrets_path)
        self.site_path = Path(self.out_path, 'CPER')
        self.year_path = Path(self.site_path, '2020')
        self.month_path = Path(self.year_path, '01')

    def test_main(self) -> None:
        pem_path = environs.Env().str('GITHUB_README_APP_PEM')
        os.environ['IN_PATH'] = str(self.data_path)
        os.environ['IN_PATH_PARSE_INDEX'] = '9'
        os.environ['LOCATION_PATH'] = str(self.location_path)
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['DB_SECRETS_PATH'] = str(self.db_secrets_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        os.environ['GITHUB_PEM'] = pem_path
        os.environ['GITHUB_APP_ID'] = '300002'
        os.environ['GITHUB_INSTALLATION_ID'] = '34765458'
        os.environ['GITHUB_HOST'] = 'https://api.github.com'
        os.environ['GITHUB_REPO_OWNER'] = 'NEONScience'
        os.environ['GITHUB_README_REPO'] = 'neon-metadata-docs'
        os.environ['GITHUB_README_PATH'] = 'readme/template.j2'
        os.environ['GITHUB_PUBLICATION_WORKBOOK_REPO'] = 'landWaterSoilIPT'
        os.environ['GITHUB_PUBLICATION_WORKBOOK_PATH'] = 'water_quality/PublicationWorkbook_Water_quality.txt'
        os.environ['GITHUB_BRANCH'] = 'NSE-9201'
        main()
        readme_count = len(list(self.month_path.glob('*.txt')))
        assert readme_count == 1
        csv_file_count = len(list(self.month_path.glob('*.csv')))
        assert csv_file_count == 7  # includes data files, variables file, and sensor_positions file.

    def tearDown(self) -> None:
        self.remove_directories()
        self.remove_database_secrets()

    def remove_directories(self):
        for path in self.month_path.glob('*'):
            if path.is_file():
                path.unlink(missing_ok=True)
        self.month_path.rmdir()
        self.year_path.rmdir()
        self.site_path.rmdir()
        self.out_path.rmdir()

    def remove_database_secrets(self):
        for path in self.db_secrets_path.glob('*'):
            path.unlink(missing_ok=True)
        self.db_secrets_path.rmdir()
