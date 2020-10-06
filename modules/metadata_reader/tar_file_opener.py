from typing import BinaryIO, Optional
import tarfile
import structlog

log = structlog.getLogger()


def open_tar(spout: BinaryIO) -> Optional[tarfile.TarFile]:
    # to use a tarfile object with a named pipe, the "w|" mode must be used
    # making it not seekable
    tar_file = None
    try:
        tar_file = tarfile.open(fileobj=spout, mode="w|", encoding='utf-8')
    except tarfile.TarError as te:
        log.error(f'error creating tar stream: {te}')
        exit(-2)
    return tar_file
