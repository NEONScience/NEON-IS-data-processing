#!/usr/bin/env python3
import os
from pathlib import Path


def filter_directory(in_path: Path, out_path: Path, filter_dirs: list, filter_dir_index: int, relative_path_index: int) -> None:
    """
    Link paths with matching directory names into the output path.
    
    :param in_path: The input path for files.
    :param out_path: The output path for linking.
    :param filter_dirs: The directories to include in the output.
    :param filter_dir_index: The path index to check for match to filter_dirs.
    :param relative_path_index: Starting index of the input path to include in the output path.
    """
    for path in in_path.rglob('*'):
        if path.is_file():
            parts = path.parts
            if len(parts) > filter_dir_index:
                name=parts[filter_dir_index]
                if name in filter_dirs:
                    link_path = Path(out_path, *path.parts[relative_path_index:])
                    link_path.parent.mkdir(parents=True, exist_ok=True)
                    if not link_path.exists():
                        link_path.symlink_to(path)
                        
