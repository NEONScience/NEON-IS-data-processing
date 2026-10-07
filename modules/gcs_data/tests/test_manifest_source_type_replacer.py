import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from gcs_data.manifest_source_type_replacer import (
    manifest_source_type_replacer,
    replace_source_type,
)


@pytest.fixture(autouse=True)
def manifest_environment(monkeypatch):
    for name in (
        "MANIFEST", "MANIFEST_FILE", "SOURCE_TYPE_OVERRIDE", "SOURCE_TYPE_TARGET"
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SOURCE_TYPE_OVERRIDE", "replacement")


def test_replace_preserves_fields_order_duplicates_and_input():
    records = [
        {
            "source_type": "cmp22",
            "data_date": "2025-10-01",
            "source_id": "11185",
            "extra": {"source_type": "nested", "values": [1, None]},
        },
        {"source_type": "other", "data_date": "2025-10"},
        {"source_type": "cmp22", "data_date": "2026"},
        {"source_type": "cmp22", "data_date": "2026"},
    ]
    original = json.loads(json.dumps(records))

    result = replace_source_type(records, "replacement")

    assert result == [dict(record, source_type="replacement") for record in original]
    assert records == original
    assert all(
        updated is not original_record
        for updated, original_record in zip(result, records)
    )


def test_empty_manifest():
    assert replace_source_type([], "replacement") == []


@pytest.mark.parametrize(
    "target, expected_types",
    [
        (None, ["replacement"] * 5),
        ("cmp22", ["replacement", "other", "replacement", "", " cmp22 "]),
        ("missing", ["cmp22", "other", "cmp22", "", " cmp22 "]),
        ("CMP22", ["cmp22", "other", "cmp22", "", " cmp22 "]),
        ("", ["cmp22", "other", "cmp22", "replacement", " cmp22 "]),
        (" cmp22 ", ["cmp22", "other", "cmp22", "", "replacement"]),
    ],
)
def test_targeted_replacement_preserves_records(target, expected_types):
    records = [
        {"source_type": source_type, "data_date": "2026", "extra": {"value": 1}}
        for source_type in ("cmp22", "other", "cmp22", "", " cmp22 ")
    ]
    original = json.loads(json.dumps(records))

    result = replace_source_type(records, "replacement", target)

    assert result == [
        dict(record, source_type=expected_type)
        for record, expected_type in zip(original, expected_types)
    ]
    assert records == original
    assert all(updated is not record for updated, record in zip(result, records))


@pytest.mark.parametrize("target", ["cmp22", "missing", ""])
def test_target_environment_variable(target, monkeypatch, capsys):
    records = [
        {"source_type": "cmp22", "data_date": "2026"},
        {"source_type": "other", "data_date": "2025-10", "source_id": "11185"},
        {"source_type": "", "data_date": "2025-10-01"},
    ]
    monkeypatch.setenv("MANIFEST", json.dumps(records))
    monkeypatch.setenv("SOURCE_TYPE_TARGET", target)

    manifest_source_type_replacer()

    assert json.loads(capsys.readouterr().out) == [
        dict(record, source_type="replacement")
        if record["source_type"] == target else record
        for record in records
    ]


@pytest.mark.parametrize("source_type", [None, 42, "", " \t\n"])
def test_invalid_replacement(source_type):
    with pytest.raises(ValueError, match="non-empty string"):
        replace_source_type([], source_type)


def test_replacement_string_is_preserved_exactly():
    result = replace_source_type(
        [{"source_type": "old", "data_date": "2026"}], " new "
    )
    assert result[0]["source_type"] == " new "


def test_inline_input_takes_priority(monkeypatch, capsys):
    monkeypatch.setenv("MANIFEST", '[{"source_type": "old", "data_date": "2026"}]')
    monkeypatch.setenv("MANIFEST_FILE", "/does/not/exist.json")

    manifest_source_type_replacer()

    output = capsys.readouterr()
    assert json.loads(output.out) == [{"source_type": "replacement", "data_date": "2026"}]
    assert output.err == ""


@pytest.mark.parametrize("inline", [None, "", " \t\n"])
def test_file_input_is_not_modified(inline, monkeypatch, tmp_path, capsys):
    manifest_file = tmp_path / "manifest.json"
    original = '[{"source_type": "old", "data_date": "2025-10", "source_id": "11185"}]'
    manifest_file.write_text(original, encoding="utf-8")
    monkeypatch.setenv("MANIFEST_FILE", str(manifest_file))
    if inline is not None:
        monkeypatch.setenv("MANIFEST", inline)

    manifest_source_type_replacer()

    assert json.loads(capsys.readouterr().out) == [
        {"source_type": "replacement", "data_date": "2025-10", "source_id": "11185"}
    ]
    assert manifest_file.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    "manifest, message",
    [
        ("{invalid", "Invalid JSON"),
        ('{"source_type": "old", "data_date": "2026"}', "JSON array"),
        ('[42]', "not an object"),
        ('[{"data_date": "2026"}]', "source_type"),
        ('[{"source_type": "old"}]', "data_date"),
        ('[{"source_type": "old", "data_date": "2026"}, null]', "not an object"),
    ],
)
def test_invalid_manifest_produces_no_output(manifest, message, monkeypatch, capsys):
    monkeypatch.setenv("MANIFEST", manifest)

    with pytest.raises(SystemExit, match=message):
        manifest_source_type_replacer()

    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("source_type", [None, "", " \t\n"])
def test_missing_or_blank_override(source_type, monkeypatch, capsys):
    monkeypatch.setenv("MANIFEST", "[]")
    if source_type is None:
        monkeypatch.delenv("SOURCE_TYPE_OVERRIDE")
    else:
        monkeypatch.setenv("SOURCE_TYPE_OVERRIDE", source_type)

    with pytest.raises(SystemExit, match="SOURCE_TYPE_OVERRIDE"):
        manifest_source_type_replacer()

    assert capsys.readouterr().out == ""


def test_missing_manifest():
    with pytest.raises(SystemExit, match="MANIFEST or MANIFEST_FILE"):
        manifest_source_type_replacer()


def test_missing_file(monkeypatch, tmp_path):
    monkeypatch.setenv("MANIFEST_FILE", str(tmp_path / "missing.json"))
    with pytest.raises(SystemExit, match="does not exist"):
        manifest_source_type_replacer()


def test_invalid_json_file(monkeypatch, tmp_path, capsys):
    manifest_file = tmp_path / "invalid.json"
    manifest_file.write_text("{invalid", encoding="utf-8")
    monkeypatch.setenv("MANIFEST_FILE", str(manifest_file))

    with pytest.raises(SystemExit, match="Invalid JSON in MANIFEST_FILE"):
        manifest_source_type_replacer()

    assert capsys.readouterr().out == ""


def test_empty_manifest_output(monkeypatch, capsys):
    monkeypatch.setenv("MANIFEST", "[]")
    manifest_source_type_replacer()
    assert json.loads(capsys.readouterr().out) == []


def test_standalone_script(monkeypatch, tmp_path):
    script = Path(__file__).resolve().parents[1] / "manifest_source_type_replacer.py"
    monkeypatch.setenv("MANIFEST", '[{"source_type": "old", "data_date": "2026"}]')

    result = subprocess.run(
        [sys.executable, str(script)],
        env=os.environ.copy(),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )

    assert json.loads(result.stdout) == [
        {"source_type": "replacement", "data_date": "2026"}
    ]
    assert result.stderr == ""