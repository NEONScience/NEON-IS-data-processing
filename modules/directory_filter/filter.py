#!/usr/bin/env python3
import os
from pathlib import Path

from lib.file_linker import link


def filter_directory(in_path, filter_dirs, out_path, relative_path_index):
    """
    Link input paths containing directory matches into the output path.

    :param in_path: The input path.
    :type in_path: str
    :param filter_dirs: The directories to filter.
    :type filter_dirs: list
    :param out_path: The output path for writing results.
    :type out_path: str
    :param relative_path_index: Starting index of the input path to include in the output path.
    :type relative_path_index: int
    :return:
    """
    for root, directories, files in os.walk(in_path):
        for name in directories:
            if not name.startswith('.') and name in filter_dirs:
                source = os.path.join(root, name)
                target = os.path.join(out_path, *Path(source).parts[relative_path_index:])
                link(source, target)
