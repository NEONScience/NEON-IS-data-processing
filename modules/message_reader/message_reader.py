import json
from typing import Callable, Iterator, Any
import structlog
import time
from pathlib import Path

from message_reader.message import Message
from message_reader.message_reader_config import Config
from message_reader.file_writer import get_write_file

log = structlog.getLogger()


def run(config: Config,
        open_pipe: Callable[[Path], Any],
        read_messages: Callable[[], Iterator[Message]]) -> None:
    write_file = get_write_file(open_pipe, config.out_path)
    while True:
        for message in read_messages():
            log.debug(f'received {message.key}  {message.value}')
            time_millis = int(round(time.time() * 1000))
            payload_id = str(message.key['payload']['id'])
            message_key = f'{time_millis}_{payload_id}'
            message_bytes = json.dumps(message.value).encode('utf-8')
            write_file(message_key, message_bytes)
            if config.is_test:
                return
