#!/usr/bin/env python3
from typing import Iterator
import json
import structlog

from kafka import KafkaConsumer

from message_reader.message_reader_config import Config
from message_reader.message import Message

log = structlog.getLogger()


def read_messages(config: Config) -> Iterator[Message]:
    consumer = create_consumer(config)
    for message in consumer:
        key = message.key
        value = message.value
        log.debug(f'message key {key}')
        log.debug(f'message value: {value}')
        yield Message(key=key, value=value)


def create_consumer(config: Config) -> KafkaConsumer:
    encoding = 'utf-8'
    topic = config.topic
    bootstrap_server = config.bootstrap_server
    auto_offset_reset = config.auto_offset_reset
    enable_auto_commit = config.enable_auto_commit
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        group_id=config.group_id,
        key_deserializer=lambda k: json.loads(k.decode(encoding)),
        value_deserializer=lambda v: json.loads(v.decode(encoding)))
    return consumer
