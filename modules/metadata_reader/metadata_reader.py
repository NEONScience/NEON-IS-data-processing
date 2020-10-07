import time
from io import BytesIO
import tarfile
from typing import Callable, Iterator, Any
from pathlib import Path
import structlog

from metadata_reader.tar_file_opener import open_tar
from metadata_reader.metadata_reader_config import Config

log = structlog.getLogger()


def read(config: Config,
         open_pipe: Callable[[Path], Any],
         read_messages: Callable[[Config], Iterator[str]]) -> None:

    # open the output path as a pipe
    out_path: Path = config.out_path

    # run continuously
    while True:
        # read messages
        for message in read_messages(config):
            log.debug(f'MESSAGE: "{message}"')
            # open output path as a named pipe
            output_pipe = open_pipe(out_path)
            if output_pipe is None:
                log.error(f'error opening output path: {out_path}')
                exit(-2)
            # create tar archive
            log.debug('creating tar archive')
            tar_stream = open_tar(output_pipe)
            # add context to tar
            tar_info = tarfile.TarInfo()
            tar_info.size = len(message)
            tar_info.mode = 0o600
            current_milliseconds = int(round(time.time() * 1000))
            tar_info.name = str(current_milliseconds)
            log.debug('writing tar file to spout')
            try:
                tar_stream.addfile(tarinfo=tar_info, fileobj=BytesIO(str(message).encode('utf-8')))
            except tarfile.TarError as te:
                log.error(f'error writing message {message} to tar file: {te}')
                exit(-2)

            # clean up
            tar_stream.close()
            output_pipe.close()

        if not config.test_mode:
            log.info('waiting for new messages')
            time.sleep(5)
        else:
            return
