#!/usr/bin/env python3
import os
from pathlib import Path

from lib.file_linker import link


def filter_directory(in_path: Path, out_path: Path, filter_dirs: list, relative_path_index: int):
    """
    Link input paths containing directory matches into the output path.

    :param in_path: The input path.
    :param out_path: The output path for writing results.
    :param filter_dirs: The directories to filter.
    :param relative_path_index: Starting index of the input path to include in the output path.
    :return:
    """
    for root, directories, files in os.walk(str(in_path)):
        for name in directories:
            if name in filter_dirs:
                path = Path(root, name)
                link_path = Path(out_path, *Path(path).parts[relative_path_index:])
                link(path, link_path)
