#!/usr/bin/env python3
import os
from pathlib import Path


def crawl(path):
    abspath = os.path.abspath(path)
    if os.path.isfile(abspath):
        yield Path(abspath)
    else:
        if not os.listdir(abspath):
            yield Path(abspath)
        else:
            for r, d, f in os.walk(path):
                for file in f:
                    if not file.startswith('.'):
                        yield Path(r, file)
