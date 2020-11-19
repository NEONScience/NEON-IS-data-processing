from typing import BinaryIO, Callable, Any
from io import BytesIO
import tarfile
import structlog
from pathlib import Path

log = structlog.getLogger()


def open_file(named_pipe: BinaryIO) -> tarfile.TarFile:
    """
    Open a tarfile for writing. To use a tarfile object with a named pipe,
    the "w|" mode must be used thus making it not seekable.

    :param named_pipe: The named pipe.
    :return: The open tarfile.
    """
    try:
        return tarfile.open(fileobj=named_pipe, mode="w|", encoding='utf-8')
    except tarfile.TarError as te:
        log.error(f'error creating tar stream: {te}')
        exit(-2)


def get_write_file(open_pipe: Callable[[Path], Any], out_path: Path) -> Callable[[str, bytes], None]:
    """
    Get the function to write files with closure bound arguments.

    :param open_pipe: Function to return a named pipe.
    :param out_path: The path to open as a named pipe.
    :return: The function.
    """
    open_pipe = open_pipe
    out_path = out_path

    def write_file(filename: str, file_bytes: bytes) -> None:
        """
        Add the file to a tar stream and write to a named pipe.

        :param filename: The filename.
        :param file_bytes: The bytes to write.
        """
        output_pipe = open_pipe(out_path)
        tar_stream = open_file(output_pipe)
        tar_info = tarfile.TarInfo()
        tar_info.size = len(file_bytes)
        tar_info.mode = 0o600
        tar_info.name = filename
        try:
            tar_stream.addfile(tarinfo=tar_info, fileobj=BytesIO(file_bytes))
        except tarfile.TarError as te:
            log.error(f'error adding {filename} to tar file: {te}')
            exit(-2)
        tar_stream.close()
        output_pipe.close()

    return write_file
