#!/usr/bin/env python3
from pathlib import Path

def get_path_key(path: Path, path_indices: list) -> str:
    """
    Create a key by concatenating path elements at the path indices.

    :param path: A path.
    :param path_indices: Path element indices to use for the key.
    :return: The key.
    """
    key = ''
    parts = path.parts
    for index in path_indices:
        part = parts[index]
        key += part
    return key
