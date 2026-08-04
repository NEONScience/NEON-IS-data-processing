#!/usr/bin/env python3.11
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
