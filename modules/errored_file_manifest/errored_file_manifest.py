#!/usr/bin/env python3.11
import json
from pathlib import Path

import structlog


def write_errored_manifest(errored_directory: Path, errored_manifest: Path) -> None:
    """
    Write a sorted list of relative file paths found under errored_directory to errored_manifest as JSON.

    :param errored_directory: Root directory to search recursively for files.
    :param errored_manifest: Output JSON file path to write the sorted list of relative paths.
    """
    log = structlog.get_logger()
    errored_files: list[str] = []
    for path in errored_directory.rglob('*'):
        if path.is_file():
            relative_path = str(path.relative_to(errored_directory))
            errored_files.append(relative_path)
            log.debug('Errored file found', errored_file=relative_path)

    sorted_errored_files = sorted(errored_files)
    errored_manifest.parent.mkdir(parents=True, exist_ok=True)
    with open(errored_manifest, "w", encoding="utf-8") as f:
        json.dump(sorted_errored_files, f)

    log.info(
        'Errored manifest written',
        errored_manifest=str(errored_manifest),
        errored_file_count=len(sorted_errored_files),
    )
