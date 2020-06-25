import os
import glob
from pathlib import Path
from typing import List


def filter_files(*, glob_pattern: str, output_path: Path) -> List[Path]:
    """
    Filter input files from the filesystem according to the given Unix style path glob pattern.

    :param glob_pattern: The path pattern to match.
    :param output_path: Path to ignore.
    :return File paths matching the given glob pattern.
    """
    files = [Path(file_path) for file_path in glob.glob(glob_pattern, recursive=True)
             if not os.path.basename(file_path).startswith(str(output_path))
             if os.path.isfile(file_path)]
    return files
