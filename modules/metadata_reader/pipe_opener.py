import os
import stat
import time


def open_pipe(path_to_file, attempts=0, timeout=3, sleep_int=5):
    if attempts < timeout:
        flags = os.O_WRONLY  # refer to "man 2 open"
        mode = stat.S_IWUSR  # this is 0o400
        u_mask = 0o777 ^ mode  # prevents always downgrading u_mask to 0
        u_mask_original = os.umask(u_mask)
        try:
            file = os.open(path_to_file, flags, mode)
            # must open the pipe as binary to prevent line-buffering problems
            return os.fdopen(file, 'wb')
        except OSError as oe:
            print(f'{attempts + 1} attempt of {timeout}. error opening file: {oe}')
            os.umask(u_mask_original)
            time.sleep(sleep_int)
            return open_pipe(path_to_file, attempts + 1)
        finally:
            os.umask(u_mask_original)
    return None
