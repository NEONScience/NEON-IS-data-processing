#!/usr/bin/env python3
import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from metadata_reader.metadata_reader import MetadataReader
from metadata_reader.kafka_reader import read_messages
from metadata_reader.metadata_reader_config import Config
from metadata_reader.pipe_opener import open_pipe


log = structlog.get_logger()


def main():
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    bootstrap_server: str = env.str('BOOTSTRAP_SERVER')
    topic: str = env.str('TOPIC')
    group_id: str = env.str('GROUP_ID')
    encoding: str = env.str('ENCODING')
    auto_offset_reset: str = env.str('AUTO_OFFSET_RESET')
    enable_auto_commit: bool = env.bool('ENABLE_AUTO_COMMIT')
    test_mode: bool = env.bool('TEST_MODE', 'False')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    config = Config(out_path=out_path,
                    bootstrap_server=bootstrap_server,
                    topic=topic,
                    group_id=group_id,
                    encoding=encoding,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=enable_auto_commit,
                    test_mode=test_mode)
    metadata_reader = MetadataReader(config)
    metadata_reader.read(open_pipe, read_messages)


if __name__ == "__main__":
    main()
