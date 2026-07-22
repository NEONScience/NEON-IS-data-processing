"""Build path selectors from a JSON manifest.

Manifest input can be provided by either:
1. MANIFEST: JSON-formatted string.
2. MANIFEST_FILE: Path to a file containing JSON.

If both are set, MANIFEST is used.

Accepted manifest format: a JSON array of objects. Each object must contain the
keys "source_type" and "data_date". The optional "source_id" key narrows the
query to a specific source; when omitted it is wildcarded. Any additional keys
in a record are ignored.

The "data_date" value may be:
- "YYYY-mm-dd": filters to an exact day.
- "YYYY-mm": filters to a month.
- "YYYY": filters to a year.

When data_date is truncated, any larger PATH indices are ignored.

Example manifest:
[
    {"source_type": "cmp22", "data_date": "2025-10-01", "source_id": "11185"},
    {"source_type": "cmp22", "data_date": "2025-10-02", "source_id": "11185"},
    {"source_type": "cmp22", "data_date": "2025-11"},
    {"source_type": "cmp22", "data_date": "2026"}
]

Output path segment order is controlled by environment variables that map fields to
0-based indexes in each output path:
- PATH_SOURCE_TYPE_INDEX
- PATH_YEAR_INDEX
- PATH_MONTH_INDEX
- PATH_DAY_INDEX
- PATH_SOURCE_ID_INDEX

For example, if:
PATH_SOURCE_TYPE_INDEX=1
PATH_YEAR_INDEX=2
PATH_MONTH_INDEX=3
PATH_DAY_INDEX=4
PATH_SOURCE_ID_INDEX=5

Then:
{
  "paths": ["source_type/year/month/day/source_id", ...]
}

So, for the example manifest, the output would be:
{
  "paths": ["cmp22/2025/10/01/11185", "cmp22/2025/10/02/11185", "cmp22/2025/11", "cmp22/2026"]
}

"""

from __future__ import annotations

from pathlib import Path
import json
import re
import sys

import environs


DATE_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$")


def _load_manifest(env: environs.Env) -> list[dict]:
    manifest_inline = env.str("MANIFEST", None)
    manifest_file_raw = env.str("MANIFEST_FILE", None)

    if manifest_inline and manifest_inline.strip():
        try:
            manifest_data = json.loads(manifest_inline)
        except json.JSONDecodeError as exc:
            sys.exit(f"Invalid JSON in MANIFEST: {exc}")
    else:
        if not manifest_file_raw:
            sys.exit("One of MANIFEST or MANIFEST_FILE environment variables is required.")
        manifest_file = Path(manifest_file_raw)
        if not manifest_file.exists():
            sys.exit(f"MANIFEST_FILE does not exist: {manifest_file}")

        with open(manifest_file, "r", encoding="utf-8") as manifest_handle:
            try:
                manifest_data = json.load(manifest_handle)
            except json.JSONDecodeError as exc:
                sys.exit(f"Invalid JSON in MANIFEST_FILE {manifest_file}: {exc}")

    if not isinstance(manifest_data, list):
        sys.exit("Manifest must be a JSON array of objects.")

    records: list[dict] = []
    for idx, record in enumerate(manifest_data):
        if not isinstance(record, dict):
            sys.exit(f"Manifest record at index {idx} is not an object.")
        if "source_type" not in record:
            sys.exit(f"Manifest record at index {idx} is missing required key 'source_type'.")
        if "data_date" not in record:
            sys.exit(f"Manifest record at index {idx} is missing required key 'data_date'.")
        records.append(record)

    return records


def _read_index_env(env: environs.Env) -> dict[str, int]:
    index_map = {
        "source_type": env.int("PATH_SOURCE_TYPE_INDEX"),
        "year": env.int("PATH_YEAR_INDEX"),
        "month": env.int("PATH_MONTH_INDEX"),
        "day": env.int("PATH_DAY_INDEX"),
        "source_id": env.int("PATH_SOURCE_ID_INDEX"),
    }

    for key, value in index_map.items():
        if value < 0:
            sys.exit(f"Index for {key} must be >= 0, got {value}.")

    if len(set(index_map.values())) != len(index_map):
        sys.exit("All OUTPUT_*_INDEX values must be unique.")

    return index_map


def _parse_data_date(data_date: object) -> tuple[str, str | None, str | None]:
    if not isinstance(data_date, str):
        sys.exit("Manifest key 'data_date' must be a string.")

    match = DATE_RE.fullmatch(data_date.strip())
    if not match:
        sys.exit(
            "Manifest key 'data_date' must match one of: YYYY-mm-dd, YYYY-mm, or YYYY."
        )

    year, month, day = match.group(1), match.group(2), match.group(3)

    if month is not None and not ("01" <= month <= "12"):
        sys.exit(f"Invalid month in data_date '{data_date}'.")
    if day is not None and not ("01" <= day <= "31"):
        sys.exit(f"Invalid day in data_date '{data_date}'.")

    return year, month, day


def _build_path(record: dict, index_map: dict[str, int]) -> str:
    source_type = record.get("source_type")
    if not isinstance(source_type, str) or not source_type.strip():
        sys.exit("Manifest key 'source_type' must be a non-empty string.")

    year, month, day = _parse_data_date(record["data_date"])

    values: dict[str, str] = {
        "source_type": source_type.strip(),
        "year": year,
    }

    if month is not None:
        values["month"] = month
    if day is not None:
        values["day"] = day

    # source_id is only used for full dates (YYYY-mm-dd).
    if day is not None:
        source_id = record.get("source_id")
        if source_id is not None:
            source_id_str = str(source_id).strip()
            if source_id_str:
                values["source_id"] = source_id_str

    indexed_values = [(index_map[key], value) for key, value in values.items()]
    indexed_values.sort(key=lambda item: item[0])

    return "/".join(value for _, value in indexed_values)


def manifest_paths_builder() -> None:
    env = environs.Env()
    index_map = _read_index_env(env)
    manifest_records = _load_manifest(env)

    seen_paths: set[str] = set()
    output_paths: list[str] = []

    for record in manifest_records:
        path = _build_path(record, index_map)
        if path not in seen_paths:
            seen_paths.add(path)
            output_paths.append(path)

    json.dump({"paths": output_paths}, sys.stdout, indent=4)
    sys.stdout.write("\n")


if __name__ == "__main__":
    manifest_paths_builder()