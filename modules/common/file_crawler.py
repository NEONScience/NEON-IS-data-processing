#!/usr/bin/env python3
import os
from pathlib import Path


def crawl(path: Path):
    """
    Yield all files in the given path.

    :param path: The path to walk.
    :return: All files in the path.
    """
    abspath = os.path.abspath(str(path))
    if os.path.isfile(abspath):
        yield Path(abspath)
    else:
        if not os.listdir(abspath):
            yield Path(abspath)
        else:
            for r, d, f in os.walk(str(path)):
                for file in f:
                    if not file.startswith('.'):
                        yield Path(r, file)
