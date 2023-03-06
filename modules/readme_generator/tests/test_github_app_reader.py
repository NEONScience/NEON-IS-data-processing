#!/usr/bin/env python3
import unittest

import environs

from readme_generator.github_app_reader import read_file, GithubConfig


class GithubReaderTest(unittest.TestCase):

    def setUp(self) -> None:
        env = environs.Env()
        pem_file_path = env.path('GITHUB_README_APP_PEM')
        self.config = GithubConfig(
            app_id='300002',
            installation_id='34765458',
            pem_file_path=pem_file_path,
            host_url='https://api.github.com',
            owner='NEONScience',
            repo='neon-metadata-docs',
            file_path='readme/readmeTemplate.txt',
            branch=None)

    @unittest.skip('Skip integration test.')
    def test_get_file(self):
        file = read_file(self.config)
        print(f'file: {file}')
