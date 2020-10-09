import os
import stat
import time
from typing import Any


def open_pipe(path, attempts=0, timeout=3, sleep_int=5) -> Any:
    """
    Open a named pipe on the path.

    :param path: The file path.
    :param attempts: Number of attempts to try and open the pipe.
    :param timeout: The timeout between attempts to open the pipe.
    :param sleep_int: The interval between attempts to open the pipe.
    :return: The open pipe file descriptor.
    """
    if attempts < timeout:
        flags = os.O_WRONLY  # refer to "man 2 open"
        mode = stat.S_IWUSR  # this is 0o400
        u_mask = 0o777 ^ mode  # prevents always downgrading u_mask to 0
        u_mask_original = os.umask(u_mask)
        try:
            file = os.open(path, flags, mode)
            # must open the pipe as binary to prevent line-buffering problems
            return os.fdopen(file, 'wb')
        except OSError as oe:
            print(f'{attempts + 1} attempt of {timeout}. error opening file: {oe}')
            os.umask(u_mask_original)
            time.sleep(sleep_int)
            return open_pipe(path, attempts + 1)
        finally:
            os.umask(u_mask_original)
    return None
