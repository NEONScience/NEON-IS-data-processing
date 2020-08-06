#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger

log = get_logger()


def group_files(*, path: Path, out_path: Path, relative_path_index: int) -> None:
    """
    Link files into the output directory.

    :param path: File or directory paths.
    :param out_path: The output path for writing results.
    :param relative_path_index: Trim path components before this index.
    """
    for path in path.rglob('*'):
        if path.is_file():
            parts = path.parts
            link_path = Path(out_path, *parts[relative_path_index:])
            log.debug(f'link: {link_path}')
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                link_path.symlink_to(path)
