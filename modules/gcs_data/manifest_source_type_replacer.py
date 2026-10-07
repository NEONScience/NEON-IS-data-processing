"""Replace manifest source_type values and write JSON to stdout.

Environment variables:
- SOURCE_TYPE_OVERRIDE (required): Non-empty replacement string.
- SOURCE_TYPE_TARGET (optional): Only replace values exactly equal to this
    string, including case and whitespace. When unset, replace every value.
    A supplied empty string matches only empty source_type values.
- MANIFEST (optional): Inline JSON array; takes precedence when non-empty.
- MANIFEST_FILE (optional): Input JSON file used when MANIFEST is unset or blank.

The manifest must contain objects with source_type and data_date keys. All other
fields, record order, and duplicate records are preserved. Input files are not
modified. Capture stdout as MANIFEST or save it to a new MANIFEST_FILE for the
L0 loader. When using an output file, unset MANIFEST so it does not take priority.

Example:
    SOURCE_TYPE_OVERRIDE=cmp22 MANIFEST_FILE=input.json \\
        python manifest_source_type_replacer.py > output.json
"""

from __future__ import annotations

import json
import sys

import environs

if __package__:
    from .manifest_paths_builder import _load_manifest
else:
    from manifest_paths_builder import _load_manifest


def replace_source_type(
    records: list[dict], source_type: str, target_source_type: str | None = None
) -> list[dict]:
    if not isinstance(source_type, str) or not source_type.strip():
        raise ValueError("SOURCE_TYPE_OVERRIDE must be a non-empty string.")
    return [
        dict(record, source_type=source_type)
        if target_source_type is None or record["source_type"] == target_source_type
        else dict(record)
        for record in records
    ]


def manifest_source_type_replacer() -> None:
    env = environs.Env()
    source_type = env.str("SOURCE_TYPE_OVERRIDE", None)
    if source_type is None:
        sys.exit("SOURCE_TYPE_OVERRIDE environment variable is required.")
    target_source_type = env.str("SOURCE_TYPE_TARGET", None)
    try:
        records = replace_source_type(
            _load_manifest(env), source_type, target_source_type
        )
    except ValueError as exc:
        sys.exit(str(exc))

    json.dump(records, sys.stdout, indent=4)
    sys.stdout.write("\n")


if __name__ == "__main__":
    manifest_source_type_replacer()