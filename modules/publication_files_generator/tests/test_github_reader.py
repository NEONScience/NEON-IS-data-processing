#!/usr/bin/env python3
import unittest

import environs

from publication_files_generator.github_reader import GithubReader


@unittest.skip('Skip integration test.')
class GithubReaderTest(unittest.TestCase):

    def setUp(self) -> None:
        env = environs.Env()
        pem_file_path = env.path('GITHUB_README_APP_PEM')
        self.github_reader = GithubReader(app_id='300002',
                                          installation_id='34765458',
                                          pem_file_path=pem_file_path,
                                          host_url='https://api.github.com',
                                          repo_owner='NEONScience',
                                          branch=None)

    def test_read_file(self):
        readme_template = self.github_reader.read_file('neon-metadata-docs', 'readme/readmeTemplate.txt')
        print(f'\nreadme_template:\n {readme_template}')
        readme_file = self.github_reader.read_file('neon-metadata-docs', 'README.md')
        print(f'\nreadme_file:\n {readme_file}')
        array_parser = self.github_reader.read_file('NEON-IS-data-processing', 'modules/array_parser/array_parser.py')
        print(f'\narray_parser:\n {array_parser}')
