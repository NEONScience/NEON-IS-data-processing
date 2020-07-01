import os
import glob
from pathlib import Path
from typing import List


def filter_paths(*, glob_pattern: str, output_path: Path) -> List[Path]:
    """
    Filter paths according to the given Unix style glob pattern.

    :param glob_pattern: The path pattern to match.
    :param output_path: Path to ignore.
    :return File paths matching the given glob pattern.
    """
    paths = [Path(file_path) for file_path in glob.glob(glob_pattern, recursive=True)
             if not os.path.basename(file_path).startswith(str(output_path))
             if os.path.isfile(file_path)]
    return paths
