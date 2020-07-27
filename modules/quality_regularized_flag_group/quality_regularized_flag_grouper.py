#!/usr/bin/env python3
from pathlib import Path
from typing import Dict, NamedTuple

from structlog import get_logger

log = get_logger()


class Paths(NamedTuple):
    path: Path
    link: Path


class QualityRegularizedFlagGrouper:

    def __init__(self, *, regularized_path: Path, quality_path: Path, out_path: Path, relative_path_index: int):
        """
        Constructor.

        :param regularized_path: The path to read regularized flag files.
        :param quality_path: The path to read quality flag files.
        :param out_path: The path to link grouped files into.
        :param relative_path_index: Include the path elements after this index in the output paths.
        """
        self.regularized_path = regularized_path
        self.quality_path = quality_path
        self.out_path = out_path
        self.relative_path_index = relative_path_index

    def group_files(self):
        regularized_files = self._load_files(self.regularized_path)
        quality_files = self._load_files(self.quality_path)
        self._link_files(regularized_files, quality_files)

    @staticmethod
    def _link_files(regularized_files: Dict[Path, Paths], quality_files: Dict[Path, Paths]):
        """
        Link matching regularized and quality files into the output directory.

        :param regularized_files: Regularized file sources and destinations.
        :param quality_files: Quality file sources and destinations.
        """
        regularized_file_keys = set(regularized_files.keys())
        quality_file_keys = set(quality_files.keys())
        log.debug(f'regularized_keys: {regularized_file_keys}')
        log.debug(f'quality_keys: {quality_file_keys}')
        common_keys = regularized_file_keys.intersection(quality_file_keys)
        log.debug(f'common: {common_keys}')
        for key in common_keys:
            regularized_paths = regularized_files.get(key)
            quality_paths = quality_files.get(key)
            regularized_file_path = regularized_paths.path
            regularized_link_path = regularized_paths.link
            regularized_link_path.parent.mkdir(parents=True, exist_ok=True)
            regularized_link_path.symlink_to(regularized_file_path)
            quality_file_path = quality_paths.path
            quality_link_path = quality_paths.link
            quality_link_path.parent.mkdir(parents=True, exist_ok=True)
            quality_link_path.symlink_to(quality_file_path)

    def _load_files(self, path: Path) -> Dict[Path, Paths]:
        """
         Associate source paths and link paths with the link directory path as a key.

        :param path: A path containing files.
        :return: File and link paths organized by key.
        """
        files: Dict[Path, Paths] = {}
        for path in path.rglob('*'):
            if path.is_file():
                file_key = Path(self.out_path, *path.parent.parts[self.relative_path_index:])
                log.debug(f'file key: {file_key}')
                link_path = Path(file_key, path.name)
                files.update({file_key: Paths(path, link_path)})
        return files
