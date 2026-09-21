#!/usr/bin/env python3
import json
import os
from pathlib import Path
from unittest import mock

from pyfakefs.fake_filesystem_unittest import TestCase

from common.path_mapper import _read_configuration, map_paths, path_mapper


class PathMapperTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()
        self.input_path = Path("/data")
        self.fs.create_file(self.input_path / "alpha/2026/09/17/id-1/a.dat")
        self.fs.create_file(self.input_path / "alpha/2026/09/18/id-1/b.dat")
        self.fs.create_file(self.input_path / "beta/2026/09/18/id-2/c.dat")

    def test_map_paths_deduplicates_and_reorders_segments(self):
        paths = map_paths(self.input_path, [0, 4], [1, 0])

        self.assertEqual(paths, ["id-1/alpha", "id-2/beta"])

    def test_map_paths_can_preserve_duplicates(self):
        paths = map_paths(self.input_path, [0, 4], [0, 1], deduplicate=False)

        self.assertEqual(paths, ["alpha/id-1", "alpha/id-1", "beta/id-2"])

    def test_map_paths_rejects_empty_indices(self):
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            map_paths(self.input_path, [], [])

    def test_map_paths_rejects_negative_indices(self):
        with self.assertRaisesRegex(ValueError, "must be >= 0"):
            map_paths(self.input_path, [0, -1], [0, 1])

    def test_map_paths_rejects_mismatched_index_list_lengths(self):
        with self.assertRaisesRegex(ValueError, "same length"):
            map_paths(self.input_path, [0, 4], [0])

    def test_map_paths_rejects_duplicate_output_indices(self):
        with self.assertRaisesRegex(ValueError, "must be unique"):
            map_paths(self.input_path, [0, 4], [0, 0])

    def test_map_paths_rejects_noncontiguous_output_indices(self):
        with self.assertRaisesRegex(ValueError, "every index"):
            map_paths(self.input_path, [0, 4], [0, 2])

    def test_path_mapper_outputs_json_and_defaults_to_deduplicate(self):
        environment = {
            "INPUT_PATH": str(self.input_path),
            "INPUT_PATH_INDICES": "0, 4",
            "OUTPUT_PATH_INDICES": "0, 1",
        }

        with mock.patch.dict(os.environ, environment, clear=True), mock.patch("sys.stdout") as stdout:
            path_mapper()

        output = "".join(call.args[0] for call in stdout.write.call_args_list)
        self.assertEqual(json.loads(output), {"paths": ["alpha/id-1", "beta/id-2"]})

    def test_rejects_duplicate_output_indices(self):
        environment = {
            "INPUT_PATH": str(self.input_path),
            "INPUT_PATH_INDICES": "0,4",
            "OUTPUT_PATH_INDICES": "0,0",
        }

        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(SystemExit, "must be unique"):
                _read_configuration()

    def test_rejects_noncontiguous_output_indices(self):
        environment = {
            "INPUT_PATH": str(self.input_path),
            "INPUT_PATH_INDICES": "0,4",
            "OUTPUT_PATH_INDICES": "0,2",
        }

        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(SystemExit, "every index"):
                _read_configuration()

    def test_rejects_mismatched_index_list_lengths(self):
        environment = {
            "INPUT_PATH": str(self.input_path),
            "INPUT_PATH_INDICES": "0,4",
            "OUTPUT_PATH_INDICES": "0",
        }

        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(SystemExit, "same length"):
                _read_configuration()

    def test_rejects_file_with_missing_input_segment(self):
        self.fs.create_file(self.input_path / "too-shallow.dat")

        with self.assertRaisesRegex(ValueError, "too few segments"):
            map_paths(self.input_path, [0, 4], [0, 1])

    def test_rejects_invalid_deduplicate_value(self):
        environment = {
            "INPUT_PATH": str(self.input_path),
            "INPUT_PATH_INDICES": "0,4",
            "OUTPUT_PATH_INDICES": "0,1",
            "DEDUPLICATE": "sometimes",
        }

        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(SystemExit, "boolean"):
                _read_configuration()