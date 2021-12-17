#!/usr/bin/env python3
import os

from pathlib import Path
from structlog import get_logger
from typing import Dict

log = get_logger()


def refactor(data_path: Path, out_path: Path, relative_path_index: int, source_id_index: int, maps: Dict) -> None:
    """
    path refactor
    :param data_path: The data input path.
    :param out_path: The output path for linking.
    :param relative_path_index: Starting index of the input path to include in the output path.
    :param source_id_index: Index of source id in the input path to be replaced with.
    """

    for path in data_path.rglob('*'):
        log.debug(f'path: {path}')
        if path.is_file():
            parts = path.parts
            source_id: str = parts[source_id_index]
            log.debug(source_id)
            if source_id in maps.keys():
                new_path = Path(str(path).replace(source_id, maps[source_id]))
                new_path = Path(out_path, *Path(new_path).parts[relative_path_index:])
            else:
                new_path = Path(out_path, *Path(path).parts[relative_path_index:])
            log.debug(f'out_path: {new_path}')
            new_path.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(path, new_path)


