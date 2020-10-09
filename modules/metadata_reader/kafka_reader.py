#!/usr/bin/env python3
from typing import Iterator, Dict
import json

from kafka import KafkaConsumer

import structlog

from metadata_reader.metadata_reader_config import Config
from metadata_reader.message import Message

log = structlog.get_logger()


def read_messages(config: Config) -> Iterator[Dict[str, str]]:
    """
    Read messages from Kafka.

    :param config: The configuration.
    :return: Yield messages.
    """
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
        key_deserializer=lambda k: k.decode('utf-8'),
        value_deserializer=lambda v: json.loads(v.decode(encoding)))
    for message in consumer:
        key = message.key
        message = message.value
        log.debug(f'kafka message received {key} {message}.')
        yield Message(key=key, content=message)
