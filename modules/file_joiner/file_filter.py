#!/usr/bin/env python3
import os
import glob


def filter_files(glob_pattern, out_path):
    """
    Filter input files from the filesystem according to the given
    Unix style path glob pattern while ignoring any files in the given output path.

    :param glob_pattern: The path pattern to match.
    :type glob_pattern: str
    :param out_path: The output path so it can be ignored when evaluating files.
    :type out_path: str
    :return list of file paths matching the given glob pattern.
    """
    file = [file_path for file_path in glob.glob(glob_pattern, recursive=True)
            if not os.path.basename(file_path).startswith(out_path)
            if os.path.isfile(file_path)]
    return file
