#!/usr/bin/env python3
from pathlib import Path
from typing import Iterator

import structlog

from data_location_group.data_location_group_config import Config
from data_location_group.data_path_parser import DataPathParser

log = structlog.get_logger()


class DataLocationGrouper:

    def __init__(self, config: Config) -> None:
        self.data_path = config.data_path
        self.location_path = config.location_path
        self.out_path = config.out_path
        self.data_path_parser = DataPathParser(config)

    def group_files(self) -> None:
        for common_path in self.link_data_files():
            self.link_location_files(common_path)

    def link_data_files(self) -> Iterator[Path]:
        """
        Link data files into the output path and yield the output directory.

        :return: Yields the output directory path for each data file.
        """
        for path in self.data_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, source_id = self.data_path_parser.parse(path)
                log.debug(f'source_id: {source_id}')
                common_path = Path(self.out_path, source_type, year, month, day, source_id)
                link_path = Path(common_path, 'data', path.name)
                log.debug(f'link path: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
                yield common_path

    def link_location_files(self, common_path: Path) -> None:
        """
        Link the location files.

        :param common_path: The common output path from data file path elements.
        """
        for path in self.location_path.rglob('*'):
            if path.is_file():
                link_path = Path(common_path, 'location', path.name)
                log.debug(f'location link path: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
