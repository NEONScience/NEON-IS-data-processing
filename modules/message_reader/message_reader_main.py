#!/usr/bin/env python3
from functools import partial

import environs
import structlog
from pathlib import Path

import common.log_config as log_config

from message_reader.message_reader import run
from message_reader.kafka_reader import read_messages
from message_reader.message_reader_config import Config
from message_reader.named_pipe import open_pipe


log = structlog.get_logger()


def main() -> None:
    env = environs.Env()
    out_path: Path = env.path('OUT_PATH')
    bootstrap_server: str = env.str('BOOTSTRAP_SERVER')
    topic: str = env.str('TOPIC')
    group_id: str = env.str('GROUP_ID')
    auto_offset_reset: str = env.str('AUTO_OFFSET_RESET')
    enable_auto_commit: bool = env.bool('ENABLE_AUTO_COMMIT')
    log_level: str = env.log_level('LOG_LEVEL', 'INFO')
    log_config.configure(log_level)
    config = Config(out_path=out_path,
                    bootstrap_server=bootstrap_server,
                    topic=topic,
                    group_id=group_id,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=enable_auto_commit,
                    is_test=False)
    read_messages_partial = partial(read_messages, config)
    run(config, open_pipe, read_messages_partial)


if __name__ == "__main__":
    main()
