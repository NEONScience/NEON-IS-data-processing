#!/usr/bin/env python3
from pathlib import Path


def link(path: Path, link_path: Path):
    """
    Symbolically link the link path to the path.

    :param path: The source path.
    :param link_path: The link path.
    """
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if not link_path.exists():
        link_path.symlink_to(path)
