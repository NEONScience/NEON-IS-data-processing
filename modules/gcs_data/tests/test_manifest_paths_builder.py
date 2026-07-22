#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest
from pyfakefs.fake_filesystem_unittest import TestCase

from gcs_data.manifest_paths_builder import (
    _build_path,
    _load_manifest,
    _parse_data_date,
    _read_index_env,
    manifest_paths_builder,
)


class ManifestPathsBuilderTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

    def test_parse_data_date_full_date(self):
        """Test parsing full date YYYY-mm-dd."""
        year, month, day = _parse_data_date("2025-10-01")
        self.assertEqual(year, "2025")
        self.assertEqual(month, "10")
        self.assertEqual(day, "01")

    def test_parse_data_date_month(self):
        """Test parsing month date YYYY-mm."""
        year, month, day = _parse_data_date("2025-11")
        self.assertEqual(year, "2025")
        self.assertEqual(month, "11")
        self.assertIsNone(day)

    def test_parse_data_date_year_only(self):
        """Test parsing year-only date YYYY."""
        year, month, day = _parse_data_date("2026")
        self.assertEqual(year, "2026")
        self.assertIsNone(month)
        self.assertIsNone(day)

    def test_parse_data_date_with_whitespace(self):
        """Test parsing date with leading/trailing whitespace."""
        year, month, day = _parse_data_date("  2025-10-01  ")
        self.assertEqual(year, "2025")
        self.assertEqual(month, "10")
        self.assertEqual(day, "01")

    def test_parse_data_date_invalid_format(self):
        """Test parsing invalid date format."""
        with self.assertRaises(SystemExit) as cm:
            _parse_data_date("25-10-01")
        self.assertIn("YYYY-mm-dd", str(cm.exception))

    def test_parse_data_date_invalid_month(self):
        """Test parsing with invalid month."""
        with self.assertRaises(SystemExit) as cm:
            _parse_data_date("2025-13-01")
        self.assertIn("Invalid month", str(cm.exception))

    def test_parse_data_date_invalid_day(self):
        """Test parsing with invalid day."""
        with self.assertRaises(SystemExit) as cm:
            _parse_data_date("2025-10-32")
        self.assertIn("Invalid day", str(cm.exception))

    def test_parse_data_date_not_string(self):
        """Test parsing with non-string data_date."""
        with self.assertRaises(SystemExit) as cm:
            _parse_data_date(20251001)
        self.assertIn("must be a string", str(cm.exception))

    def test_read_index_env_valid(self):
        """Test reading valid index environment variables."""
        os.environ["PATH_SOURCE_TYPE_INDEX"] = "0"
        os.environ["PATH_YEAR_INDEX"] = "1"
        os.environ["PATH_MONTH_INDEX"] = "2"
        os.environ["PATH_DAY_INDEX"] = "3"
        os.environ["PATH_SOURCE_ID_INDEX"] = "4"

        import environs
        env = environs.Env()
        index_map = _read_index_env(env)

        self.assertEqual(index_map["source_type"], 0)
        self.assertEqual(index_map["year"], 1)
        self.assertEqual(index_map["month"], 2)
        self.assertEqual(index_map["day"], 3)
        self.assertEqual(index_map["source_id"], 4)

    def test_read_index_env_negative_index(self):
        """Test that negative indices cause an error."""
        os.environ["PATH_SOURCE_TYPE_INDEX"] = "-1"
        os.environ["PATH_YEAR_INDEX"] = "1"
        os.environ["PATH_MONTH_INDEX"] = "2"
        os.environ["PATH_DAY_INDEX"] = "3"
        os.environ["PATH_SOURCE_ID_INDEX"] = "4"

        import environs
        env = environs.Env()
        with self.assertRaises(SystemExit) as cm:
            _read_index_env(env)
        self.assertIn("must be >= 0", str(cm.exception))

    def test_read_index_env_duplicate_indices(self):
        """Test that duplicate indices cause an error."""
        os.environ["PATH_SOURCE_TYPE_INDEX"] = "0"
        os.environ["PATH_YEAR_INDEX"] = "0"
        os.environ["PATH_MONTH_INDEX"] = "2"
        os.environ["PATH_DAY_INDEX"] = "3"
        os.environ["PATH_SOURCE_ID_INDEX"] = "4"

        import environs
        env = environs.Env()
        with self.assertRaises(SystemExit) as cm:
            _read_index_env(env)
        self.assertIn("must be unique", str(cm.exception))

    def test_load_manifest_from_inline(self):
        """Test loading manifest from inline JSON in MANIFEST env var."""
        manifest_data = [
            {"source_type": "cmp22", "data_date": "2025-10-01", "source_id": "11185"},
            {"source_type": "cmp22", "data_date": "2025-10-02"},
        ]
        os.environ["MANIFEST"] = json.dumps(manifest_data)

        import environs
        env = environs.Env()
        records = _load_manifest(env)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["source_type"], "cmp22")
        self.assertEqual(records[0]["source_id"], "11185")

    def test_load_manifest_from_file(self):
        """Test loading manifest from file."""
        manifest_data = [
            {"source_type": "cmp22", "data_date": "2025-10-01"},
        ]
        manifest_file = Path("/manifest.json")
        with open(manifest_file, "w") as f:
            json.dump(manifest_data, f)

        os.environ.pop("MANIFEST", None)
        os.environ["MANIFEST_FILE"] = str(manifest_file)

        import environs
        env = environs.Env()
        records = _load_manifest(env)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["source_type"], "cmp22")

    def test_load_manifest_missing_required_field(self):
        """Test that missing required fields cause an error."""
        manifest_data = [
            {"source_type": "cmp22"},  # Missing data_date
        ]
        os.environ["MANIFEST"] = json.dumps(manifest_data)

        import environs
        env = environs.Env()
        with self.assertRaises(SystemExit) as cm:
            _load_manifest(env)
        self.assertIn("data_date", str(cm.exception))

    def test_load_manifest_not_array(self):
        """Test that non-array manifest causes an error."""
        os.environ["MANIFEST"] = json.dumps({"source_type": "cmp22", "data_date": "2025-10-01"})

        import environs
        env = environs.Env()
        with self.assertRaises(SystemExit) as cm:
            _load_manifest(env)
        self.assertIn("must be a JSON array", str(cm.exception))

    def test_load_manifest_invalid_json(self):
        """Test that invalid JSON causes an error."""
        os.environ["MANIFEST"] = "{ invalid json }"

        import environs
        env = environs.Env()
        with self.assertRaises(SystemExit) as cm:
            _load_manifest(env)
        self.assertIn("Invalid JSON", str(cm.exception))

    def test_load_manifest_file_not_found(self):
        """Test that missing file causes an error."""
        os.environ.pop("MANIFEST", None)
        os.environ["MANIFEST_FILE"] = "/nonexistent.json"

        import environs
        env = environs.Env()
        with self.assertRaises(SystemExit) as cm:
            _load_manifest(env)
        self.assertIn("does not exist", str(cm.exception))

    def test_load_manifest_priority_inline_over_file(self):
        """Test that MANIFEST is used when both MANIFEST and MANIFEST_FILE are set."""
        inline_data = [{"source_type": "inline", "data_date": "2025-01-01"}]
        file_data = [{"source_type": "file", "data_date": "2025-02-01"}]

        os.environ["MANIFEST"] = json.dumps(inline_data)
        manifest_file = Path("/manifest.json")
        with open(manifest_file, "w") as f:
            json.dump(file_data, f)
        os.environ["MANIFEST_FILE"] = str(manifest_file)

        import environs
        env = environs.Env()
        records = _load_manifest(env)

        self.assertEqual(records[0]["source_type"], "inline")

    def test_build_path_full_date_with_source_id(self):
        """Test building path with full date and source_id."""
        record = {
            "source_type": "cmp22",
            "data_date": "2025-10-01",
            "source_id": "11185",
        }
        index_map = {
            "source_type": 0,
            "year": 1,
            "month": 2,
            "day": 3,
            "source_id": 4,
        }
        path = _build_path(record, index_map)
        self.assertEqual(path, "cmp22/2025/10/01/11185")

    def test_build_path_full_date_without_source_id(self):
        """Test building path with full date but no source_id."""
        record = {
            "source_type": "cmp22",
            "data_date": "2025-10-01",
        }
        index_map = {
            "source_type": 0,
            "year": 1,
            "month": 2,
            "day": 3,
            "source_id": 4,
        }
        path = _build_path(record, index_map)
        self.assertEqual(path, "cmp22/2025/10/01")

    def test_build_path_month_only_date(self):
        """Test building path with month-only date (source_id ignored)."""
        record = {
            "source_type": "cmp22",
            "data_date": "2025-11",
            "source_id": "11185",
        }
        index_map = {
            "source_type": 0,
            "year": 1,
            "month": 2,
            "day": 3,
            "source_id": 4,
        }
        path = _build_path(record, index_map)
        # source_id should not be included when day is missing
        self.assertEqual(path, "cmp22/2025/11")

    def test_build_path_year_only_date(self):
        """Test building path with year-only date."""
        record = {
            "source_type": "cmp22",
            "data_date": "2026",
        }
        index_map = {
            "source_type": 0,
            "year": 1,
            "month": 2,
            "day": 3,
            "source_id": 4,
        }
        path = _build_path(record, index_map)
        self.assertEqual(path, "cmp22/2026")

    def test_build_path_custom_index_order(self):
        """Test building path with custom index ordering (source_id at end)."""
        record = {
            "source_type": "cmp22",
            "data_date": "2025-10-01",
            "source_id": "11185",
        }
        index_map = {
            "source_type": 0,
            "year": 1,
            "month": 2,
            "day": 3,
            "source_id": 5,  # source_id at higher index
        }
        path = _build_path(record, index_map)
        self.assertEqual(path, "cmp22/2025/10/01/11185")

    def test_build_path_empty_source_type(self):
        """Test that empty source_type causes an error."""
        record = {
            "source_type": "   ",
            "data_date": "2025-10-01",
        }
        index_map = {
            "source_type": 0,
            "year": 1,
            "month": 2,
            "day": 3,
            "source_id": 4,
        }
        with self.assertRaises(SystemExit) as cm:
            _build_path(record, index_map)
        self.assertIn("non-empty string", str(cm.exception))

    def test_build_path_empty_source_id_ignored(self):
        """Test that empty source_id is ignored."""
        record = {
            "source_type": "cmp22",
            "data_date": "2025-10-01",
            "source_id": "   ",
        }
        index_map = {
            "source_type": 0,
            "year": 1,
            "month": 2,
            "day": 3,
            "source_id": 4,
        }
        path = _build_path(record, index_map)
        self.assertEqual(path, "cmp22/2025/10/01")

    def test_manifest_paths_builder_basic(self):
        """Test end-to-end manifest_paths_builder with basic example."""
        manifest_data = [
            {"source_type": "cmp22", "data_date": "2025-10-01", "source_id": "11185"},
            {"source_type": "cmp22", "data_date": "2025-10-02", "source_id": "11185"},
            {"source_type": "cmp22", "data_date": "2025-11"},
            {"source_type": "cmp22", "data_date": "2026"},
        ]
        os.environ["MANIFEST"] = json.dumps(manifest_data)
        os.environ["PATH_SOURCE_TYPE_INDEX"] = "0"
        os.environ["PATH_YEAR_INDEX"] = "1"
        os.environ["PATH_MONTH_INDEX"] = "2"
        os.environ["PATH_DAY_INDEX"] = "3"
        os.environ["PATH_SOURCE_ID_INDEX"] = "4"

        # Capture stdout
        from io import StringIO
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            manifest_paths_builder()
            output = captured_output.getvalue()
            result = json.loads(output)

            self.assertIn("paths", result)
            paths = result["paths"]
            self.assertEqual(len(paths), 4)
            self.assertIn("cmp22/2025/10/01/11185", paths)
            self.assertIn("cmp22/2025/10/02/11185", paths)
            self.assertIn("cmp22/2025/11", paths)
            self.assertIn("cmp22/2026", paths)
        finally:
            sys.stdout = sys.__stdout__

    def test_manifest_paths_builder_deduplication(self):
        """Test that duplicate paths are deduplicated."""
        manifest_data = [
            {"source_type": "cmp22", "data_date": "2025-10-01", "source_id": "11185"},
            {"source_type": "cmp22", "data_date": "2025-10-01", "source_id": "11185"},
        ]
        os.environ["MANIFEST"] = json.dumps(manifest_data)
        os.environ["PATH_SOURCE_TYPE_INDEX"] = "0"
        os.environ["PATH_YEAR_INDEX"] = "1"
        os.environ["PATH_MONTH_INDEX"] = "2"
        os.environ["PATH_DAY_INDEX"] = "3"
        os.environ["PATH_SOURCE_ID_INDEX"] = "4"

        from io import StringIO
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            manifest_paths_builder()
            output = captured_output.getvalue()
            result = json.loads(output)

            paths = result["paths"]
            # Should have only one path, not two
            self.assertEqual(len(paths), 1)
            self.assertEqual(paths[0], "cmp22/2025/10/01/11185")
        finally:
            sys.stdout = sys.__stdout__

    def test_manifest_paths_builder_multiple_source_types(self):
        """Test with multiple source types."""
        manifest_data = [
            {"source_type": "cmp22", "data_date": "2025-10-01", "source_id": "11185"},
            {"source_type": "co2", "data_date": "2025-10-01", "source_id": "12345"},
        ]
        os.environ["MANIFEST"] = json.dumps(manifest_data)
        os.environ["PATH_SOURCE_TYPE_INDEX"] = "0"
        os.environ["PATH_YEAR_INDEX"] = "1"
        os.environ["PATH_MONTH_INDEX"] = "2"
        os.environ["PATH_DAY_INDEX"] = "3"
        os.environ["PATH_SOURCE_ID_INDEX"] = "4"

        from io import StringIO
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            manifest_paths_builder()
            output = captured_output.getvalue()
            result = json.loads(output)

            paths = result["paths"]
            self.assertEqual(len(paths), 2)
            self.assertIn("cmp22/2025/10/01/11185", paths)
            self.assertIn("co2/2025/10/01/12345", paths)
        finally:
            sys.stdout = sys.__stdout__
