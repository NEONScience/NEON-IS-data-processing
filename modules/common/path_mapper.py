"""Map selected segments from file paths into a new path layout.

Environment variables:
- INPUT_PATH: Root directory containing the files to map.
- INPUT_PATH_INDICES: Comma-separated, zero-based source segment indices.
- OUTPUT_PATH_INDICES: Comma-separated, zero-based destination segment indices.
- DEDUPLICATE: Optional boolean controlling duplicate removal (default: true).

The resulting paths are written to stdout as ``{"paths": [...]}`` JSON.

Example:
    Given files beneath INPUT_PATH=/data:
        alpha/2026/09/17/id-1/a.dat
        alpha/2026/09/18/id-1/b.dat
        beta/2026/09/18/id-2/c.dat

    With INPUT_PATH_INDICES=0,4 and OUTPUT_PATH_INDICES=1,0, the output is:
        {"paths": ["id-1/alpha", "id-2/beta"]}

To use from other Python modules instead of via the CLI/environment variables,
import and call ``map_paths`` directly to get a list back:

    from common.path_mapper import map_paths
    paths = map_paths(Path("/data"), [0, 4], [1, 0])
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys


def _parse_indices(name: str) -> list[int]:
    raw_value = os.environ.get(name)
    if raw_value is None or not raw_value.strip():
        sys.exit(f"{name} environment variable is required.")

    try:
        indices = [int(value.strip()) for value in raw_value.split(",")]
    except ValueError:
        sys.exit(f"{name} must be a comma-separated list of integers.")

    if any(index < 0 for index in indices):
        sys.exit(f"All {name} values must be >= 0.")

    return indices


def _parse_deduplicate() -> bool:
    raw_value = os.environ.get("DEDUPLICATE", "true").strip().lower()
    if raw_value in {"true", "1", "yes", "on"}:
        return True
    if raw_value in {"false", "0", "no", "off"}:
        return False
    sys.exit("DEDUPLICATE must be a boolean value.")


def _read_configuration() -> tuple[Path, list[int], list[int], bool]:
    input_path_raw = os.environ.get("INPUT_PATH")
    if input_path_raw is None or not input_path_raw.strip():
        sys.exit("INPUT_PATH environment variable is required.")

    input_path = Path(input_path_raw)
    if not input_path.is_dir():
        sys.exit(f"INPUT_PATH is not a directory: {input_path}")

    input_indices = _parse_indices("INPUT_PATH_INDICES")
    output_indices = _parse_indices("OUTPUT_PATH_INDICES")

    if len(input_indices) != len(output_indices):
        sys.exit("INPUT_PATH_INDICES and OUTPUT_PATH_INDICES must have the same length.")
    if len(set(output_indices)) != len(output_indices):
        sys.exit("OUTPUT_PATH_INDICES values must be unique.")
    if set(output_indices) != set(range(len(output_indices))):
        sys.exit("OUTPUT_PATH_INDICES must contain every index from 0 through list length minus one.")

    return input_path, input_indices, output_indices, _parse_deduplicate()


def map_paths(
    input_path: Path,
    input_indices: list[int],
    output_indices: list[int],
    deduplicate: bool = True,
) -> list[str]:
    """Map relative file path segments into the configured output layout."""
    mapped_paths: list[str] = []

    for file_path in sorted(path for path in input_path.rglob("*") if path.is_file()):
        input_parts = file_path.relative_to(input_path).parts
        if max(input_indices) >= len(input_parts):
            sys.exit(
                f"File path has too few segments for INPUT_PATH_INDICES: "
                f"{file_path.relative_to(input_path)}"
            )

        output_parts = [""] * len(output_indices)
        for input_index, output_index in zip(input_indices, output_indices):
            output_parts[output_index] = input_parts[input_index]
        mapped_paths.append("/".join(output_parts))

    if deduplicate:
        return sorted(set(mapped_paths))
    return mapped_paths


def path_mapper() -> None:
    input_path, input_indices, output_indices, deduplicate = _read_configuration()
    paths = map_paths(input_path, input_indices, output_indices, deduplicate)
    json.dump({"paths": paths}, sys.stdout, indent=4)
    sys.stdout.write("\n")


if __name__ == "__main__":
    path_mapper()