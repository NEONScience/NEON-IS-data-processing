#!/usr/bin/env python3
from pathlib import Path

from structlog import get_logger

log = get_logger()


class QualityRegularizedFlagGrouper(object):

    def __init__(self, *, regularized_path: Path, quality_path: Path, out_path: Path, relative_path_index: int):
        self.regularized_path = regularized_path
        self.quality_path = quality_path
        self.out_path = out_path
        self.relative_path_index = relative_path_index

    def group_files(self):
        regularized_files = self.load_files(self.regularized_path)
        quality_files = self.load_files(self.quality_path)
        self.link_files(regularized_files, quality_files)

    @staticmethod
    def link_files(regularized_files: dict, quality_files: dict):
        """
        Group matching regularized and quality files in the output directory.

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
            regularized_file_path = Path(regularized_paths.get('file_path'))
            regularized_link_path = Path(regularized_paths.get('link_path'))
            regularized_link_path.parent.mkdir(parents=True, exist_ok=True)
            regularized_link_path.symlink_to(regularized_file_path)
            quality_file_path = Path(quality_paths.get('file_path'))
            quality_link_path = Path(quality_paths.get('link_path'))
            quality_link_path.parent.mkdir(parents=True, exist_ok=True)
            quality_link_path.symlink_to(quality_file_path)

    def load_files(self, path: Path):
        """
        Read files and generate keys associated to the file paths and generated link paths.

        :param path: A path containing files.
        :return: File and link paths organized by key.
        """
        files = {}
        for path in path.rglob('*'):
            if path.is_file():
                file_key = Path(self.out_path, *path.parent.parts[self.relative_path_index:])
                log.debug(f'file key: {file_key}')
                link_path = Path(file_key, path.name)
                file_paths = {'file_path': path, 'link_path': link_path}
                files.update({file_key: file_paths})
                log.debug(f'adding key: {file_key} value: {file_paths}')
        return files
