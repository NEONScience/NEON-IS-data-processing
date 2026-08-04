#!/usr/bin/env python3.11
"""Generate a manifest of errored files.

Inputs (environment variables):
- ERRORED_DIRECTORY: Path to the directory containing errored files.
- ERRORED_MANIFEST: Path where the manifest file will be written.
- LOG_LEVEL (optional): Logging level (default: INFO).

Output:
- Writes an errored-file manifest to ERRORED_MANIFEST.

Example:
    export ERRORED_DIRECTORY='/data/errored_datums"
    export ERRORED_MANIFEST="/data/errored_manifest.txt"
    export LOG_LEVEL="INFO"
    python3 modules/errored_file_manifest/errored_file_manifest_main.py
"""

import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from errored_file_manifest.errored_file_manifest import write_errored_manifest


def main() -> None:
    env = environs.Env()
    errored_directory: Path = env.path('ERRORED_DIRECTORY')
    errored_manifest: Path = env.path('ERRORED_MANIFEST')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    log = structlog.get_logger()
    log.debug(f'errored_directory: {errored_directory} errored_manifest: {errored_manifest}')
    write_errored_manifest(errored_directory, errored_manifest)


if __name__ == '__main__':
    main()
