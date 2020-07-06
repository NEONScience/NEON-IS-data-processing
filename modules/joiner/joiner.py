#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger

log = get_logger()


def join_files(*, related_paths: list, out_path: Path, relative_path_index: int):
    """
    Link files in related paths into the output directory.

    :param related_paths: Paths containing files to process.
    :param out_path: The output path for linking files.
    :param relative_path_index: Trim the input path to this index.
    """
    for related_path in related_paths:
        for path in related_path.rglob('*'):
            if path.is_file():
                parts = path.parts
                link_path = Path(out_path, *parts[relative_path_index:])
                log.debug(f'path: {path} link: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
