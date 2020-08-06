#!/usr/bin/env python3
from pathlib import Path
import structlog

from event_location_group.event_location_group_config import Config
from event_location_group.data_path_parser import DataPathParser

log = structlog.get_logger()


class EventLocationGrouper:

    def __init__(self, config: Config) -> None:
        self.data_path = config.data_path
        self.location_path = config.location_path
        self.out_path = config.out_path
        self.data_path_parser = DataPathParser(config)

    def group_files(self) -> None:
        """Link event data and location files into output path."""
        for path in self.data_path.rglob('*'):
            if path.is_file():
                source_type, year, month, day, source_id = self.data_path_parser.parse(path)
                log.debug(f'file: {path.name} source_type: {source_type} source_id: {source_id}')
                link_root_path = Path(self.out_path, source_type, year, month, day, source_id)
                self.link_location(link_root_path)
                link_path = Path(link_root_path, 'data', path.name)
                log.debug(f'data link: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)

    def link_location(self, link_root_path: Path) -> None:
        """
        Link the location file into the target directory.

        :param link_root_path: The target directory path.
        """
        for path in self.location_path.rglob('*'):
            if path.is_file():
                link_path = Path(link_root_path, 'location', path.name)
                log.debug(f'location link: {link_path}')
                link_path.parent.mkdir(parents=True, exist_ok=True)
                if not link_path.exists():
                    link_path.symlink_to(path)
