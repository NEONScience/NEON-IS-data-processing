#!/usr/bin/env python3
import json
from pathlib import Path

from pyfakefs.fake_filesystem_unittest import TestCase

from errored_file_manifest.errored_file_manifest import write_errored_manifest


class ErroredFileManifestTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.errored_directory = Path('/errored')
        self.errored_manifest = Path('/outputs/errored_manifest.json')

    def test_recursive_relative_path_discovery(self):
        self.fs.create_file(self.errored_directory.joinpath('a.ext'))
        self.fs.create_file(self.errored_directory.joinpath('sub', 'b.ext'))
        self.fs.create_file(self.errored_directory.joinpath('sub', 'nested', 'c.ext'))

        write_errored_manifest(self.errored_directory, self.errored_manifest)

        manifest = json.loads(self.errored_manifest.read_text(encoding='utf-8'))
        self.assertEqual(manifest, ['a.ext', 'sub/b.ext', 'sub/nested/c.ext'])

    def test_deterministic_sorting(self):
        self.fs.create_file(self.errored_directory.joinpath('c.ext'))
        self.fs.create_file(self.errored_directory.joinpath('a.ext'))
        self.fs.create_file(self.errored_directory.joinpath('b.ext'))

        write_errored_manifest(self.errored_directory, self.errored_manifest)

        manifest = json.loads(self.errored_manifest.read_text(encoding='utf-8'))
        self.assertEqual(manifest, ['a.ext', 'b.ext', 'c.ext'])

    def test_empty_input(self):
        self.fs.create_dir(self.errored_directory)

        write_errored_manifest(self.errored_directory, self.errored_manifest)

        manifest = json.loads(self.errored_manifest.read_text(encoding='utf-8'))
        self.assertEqual(manifest, [])

    def test_parent_directory_creation(self):
        self.fs.create_file(self.errored_directory.joinpath('a.ext'))
        nested_manifest = Path('/outputs/nested/deep/errored_manifest.json')
        self.assertFalse(nested_manifest.parent.exists())

        write_errored_manifest(self.errored_directory, nested_manifest)

        self.assertTrue(nested_manifest.parent.exists())
        manifest = json.loads(nested_manifest.read_text(encoding='utf-8'))
        self.assertEqual(manifest, ['a.ext'])
