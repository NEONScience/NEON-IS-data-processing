import time
from io import BytesIO
import tarfile
import json
from typing import Callable, Iterator, Any
from pathlib import Path
import structlog

from metadata_reader.tar_file_opener import open_tar
from metadata_reader.message import Message
from metadata_reader.metadata_reader_config import Config

log = structlog.getLogger()


class MetadataReader:

    def __init__(self, config: Config):
        self.config = config
        self.out_path = config.out_path
        self.test_mode = config.test_mode
        self.run = True

    def read(self, open_pipe: Callable[[Path], Any],
             read_messages: Callable[[Config], Iterator[Message]]) -> None:
        while self.run:
            for message in read_messages(self.config):
                key = message.key
                content = message.content
                log.debug(f'key: {key} content: "{content}"')
                content_bytes = json.dumps(content).encode('utf-8')
                # open output path as a named pipe
                output_pipe = open_pipe(self.out_path)
                if output_pipe is None:
                    log.error(f'error opening output path: {self.out_path}')
                    exit(-2)
                # create tar archive
                log.debug('creating tar archive')
                tar_stream = open_tar(output_pipe)
                # add context to tar
                tar_info = tarfile.TarInfo()
                tar_info.size = len(content_bytes)
                tar_info.mode = 0o600
                # current_milliseconds = int(round(time.time() * 1000))
                # tar_info.name = str(current_milliseconds)
                tar_info.name = key
                log.debug('writing tar file to spout')
                try:
                    log.debug(f'message bytes: {content_bytes}')
                    tar_stream.addfile(tarinfo=tar_info, fileobj=BytesIO(content_bytes))
                except tarfile.TarError as te:
                    log.error(f'error writing message {key} to tar file: {te}')
                    exit(-2)
                # clean up to write file
                tar_stream.close()
                output_pipe.close()

            if not self.test_mode:
                log.info('waiting for new messages')
                time.sleep(5)
            else:
                return

    def shutdown(self):
        self.run = False
