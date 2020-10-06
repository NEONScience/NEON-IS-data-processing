#!/usr/bin/env python3
from typing import Iterator
from json import loads

from kafka import KafkaConsumer

import structlog

from metadata_reader.metadata_reader_config import Config

log = structlog.get_logger()


def read_messages(config: Config) -> Iterator[str]:
    """Write location files into the output path."""
    topic = config.topic
    bootstrap_server = config.bootstrap_server
    auto_offset_reset = config.auto_offset_reset
    enable_auto_commit = config.enable_auto_commit
    encoding = config.encoding
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        group_id=config.group_id,
        value_deserializer=lambda x: loads(x.decode(encoding)))
    for message in consumer:
        message = message.value
        print(f'{message} received.')
        yield message
