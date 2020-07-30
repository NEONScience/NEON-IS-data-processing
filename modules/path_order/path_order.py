#!/usr/bin/env python3
from pathlib import Path

import structlog

log = structlog.get_logger()


def order_paths(in_path: Path, out_path: Path, indices: list):
    """
    Re-order a path into the sequence defined by the indices and link the path
    to the new path in the output directory.

    :param in_path: A path containing files.
    :param out_path: The output path for linking files.
    :param indices: The desired path element sequence.
    """
    for path in in_path.rglob('*'):
        if path.is_file():
            link_path = order_path(path, indices, out_path)
            link_path.parent.mkdir(parents=True, exist_ok=True)
            if not link_path.exists():
                log.debug(f'path: {path} link_path: {link_path}')
                link_path.symlink_to(path)


def order_path(path: Path, indices: list, base_path: Path) -> Path:
    """
    Re-order a path into a new path based on the indices.

    :param path: The source path.
    :param indices: The desired path element sequence.
    :param base_path: The base root for the new path.
    :return: The new re-ordered path.
    """
    path_parts = path.parts
    new_path = Path(base_path)
    for index in indices:
        part = path_parts[int(index)]
        new_path = new_path.joinpath(part)
    return new_path
