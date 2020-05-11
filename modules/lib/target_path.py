#!/usr/bin/env python3
import os
from pathlib import Path


def get_path(source_path, out_path):
    """
    Remove root and repo name from the source path
    and prepend the output directory.

    :param source_path: The source file path.
    :type source_path: str
    :param out_path: The output directory to be prepended.
    :type out_path: str
    :return: The full output path str.
    """
    trimmed_path = trim_path(Path(source_path))
    target_path = os.path.join(out_path, trimmed_path)
    return target_path


def trim_path(path):
    """
    Trim root and repo name from the path.

    :param path: An input path
    :type path: pathlib.Path object
    :return: The path str without the root and repo name elements.
    """
    return Path(*path.parts[3:])
