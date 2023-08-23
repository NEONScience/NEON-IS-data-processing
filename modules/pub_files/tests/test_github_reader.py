#!/usr/bin/env python3
import unittest

import environs

from pub_files.external_files.github_config import GithubConfig
from pub_files.external_files.github_reader import make_read_file


@unittest.skip('Skip integration test.')
class GithubReaderTest(unittest.TestCase):

    def setUp(self) -> None:
        env = environs.Env()
        certificate_path = env.path('GITHUB_README_APP_PEM')
        self.config = GithubConfig(app_id='300002',
                              installation_id='34765458',
                              certificate_path=certificate_path,
                              host='https://api.github.com',
                              repo_owner='NEONScience',
                              branch=None)

    def test_read_file(self):
        read_file = make_read_file(self.config)
        readme_template = read_file('neon-metadata-docs', 'readme/readmeTemplate.txt')
        readme_file = read_file('neon-metadata-docs', 'README.md')
        array_parser = read_file('NEON-IS-data-processing', 'modules/array_parser/array_parser.py')
        print(f'\nreadme_template:\n {readme_template}')
        print(f'\nreadme_file:\n {readme_file}')
        print(f'\narray_parser:\n {array_parser}')
