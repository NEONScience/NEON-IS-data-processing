#!/usr/bin/env python3
import os
from pathlib import Path


def filter_directory(in_path: Path, out_path: Path, filter_dirs: list, relative_path_index: int) -> None:
    """
    Link paths with matching directory names into the output path.

    :param in_path: The input path for files.
    :param out_path: The output path for linking.
    :param filter_dirs: The directories to filter_paths.
    :param relative_path_index: Starting index of the input path to include in the output path.
    """
    for root, directories, files in os.walk(str(in_path)):
        for name in directories:
            if name in filter_dirs:
                path = Path(root, name)
                link_path = Path(out_path, *Path(path).parts[relative_path_index:])
                link_path.parent.mkdir(parents=True, exist_ok=True)
                link_path.symlink_to(path)
