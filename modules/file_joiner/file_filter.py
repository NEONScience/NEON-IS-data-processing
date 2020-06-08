#!/usr/bin/env python3
import os
from pathlib import Path
import glob


def filter_files(glob_pattern: str, out_path: Path):
    """
    Filter input files from the filesystem according to the given
    Unix style path glob pattern while ignoring any files in the given output path.

    :param glob_pattern: The path pattern to contains_match.
    :param out_path: The output path so it can be ignored when evaluating files.
    :return File paths matching the given glob pattern.
    """
    files = [file_path for file_path in glob.glob(glob_pattern, recursive=True)
             if not os.path.basename(file_path).startswith(str(out_path))
             if os.path.isfile(file_path)]
    return files
